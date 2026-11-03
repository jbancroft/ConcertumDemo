package minuetsql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	minuetindex "github.com/jbancroft/ConcertumDemo/minuet-index"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// Catalog capacity, table region allocation, and stored catalog validation.

const smallTableRegionSize = 64 + 3*512 // Superblock, two leaves, and a root; 12 ascending rows fit.

func newRegionTestEngine(t *testing.T, client sinfoniaclient.TxnClient, regionSize, memorySize uint64) *Engine {
	t.Helper()
	e, err := NewEngine(Config{TxnClient: client, TableRegionSize: regionSize, MemorySize: memorySize})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func execOK(t *testing.T, e *Engine, sql string) QueryResult {
	t.Helper()
	result, err := e.Exec(t.Context(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return result
}

func fillAdjacentTables(t *testing.T, e *Engine) {
	t.Helper()
	for _, name := range []string{"alpha", "beta"} {
		execOK(t, e, "CREATE TABLE "+name)
	}
	for key := 1; key <= 12; key++ {
		execOK(t, e, fmt.Sprintf("INSERT INTO alpha VALUES (%d, %d)", key, key*10))
		execOK(t, e, fmt.Sprintf("INSERT INTO beta VALUES (%d, %d)", key, key*100))
	}
}

func checkAdjacentTables(t *testing.T, e *Engine) {
	t.Helper()
	for i, name := range []string{"alpha", "beta"} {
		result := execOK(t, e, "SELECT id, value FROM "+name)
		if len(result.Rows) != 12 {
			t.Fatalf("%s: got %d rows, want 12", name, len(result.Rows))
		}
		factor := uint64(10)
		if i == 1 {
			factor = 100
		}
		for row, got := range result.Rows {
			id := uint64(row + 1)
			if got != (Row{ID: id, Value: id * factor}) {
				t.Fatalf("%s row %d: %+v", name, row, got)
			}
		}
	}
}

func TestTableRegionsStayIsolatedAndPersistOnReopen(t *testing.T) {
	client := newMemTxnClient()
	e := newRegionTestEngine(t, client, smallTableRegionSize, testMemorySize)
	fillAdjacentTables(t, e)
	checkAdjacentTables(t, e)
	before := bytes.Clone(client.ram)
	if _, err := e.Exec(t.Context(), "INSERT INTO alpha VALUES (13, 130)"); !errors.Is(err, ErrTableFull) {
		t.Fatalf("full table insert: %v", err)
	}
	if !bytes.Equal(before, client.ram) {
		t.Fatal("full table changed storage")
	}
	// The new default applies only to future tables. Existing tables retain
	// their stored bounds and data, including when the engine reopens them.
	e = newRegionTestEngine(t, client, 2*smallTableRegionSize, testMemorySize)
	checkAdjacentTables(t, e)
	if _, err := e.Exec(t.Context(), "INSERT INTO alpha VALUES (13, 130)"); !errors.Is(err, ErrTableFull) {
		t.Fatalf("reopened table lost its original limit: %v", err)
	}
	execOK(t, e, "CREATE TABLE gamma")
	for key := 1; key <= 13; key++ {
		execOK(t, e, fmt.Sprintf("INSERT INTO gamma VALUES (%d, %d)", key, key))
	}
	checkAdjacentTables(t, e)
	result := execOK(t, e, "SELECT id, value FROM gamma")
	if len(result.Rows) != 13 {
		t.Fatalf("larger new table has %d rows", len(result.Rows))
	}
	// Deleting from a full neighbor remains possible and leaves alpha intact.
	execOK(t, e, "DELETE FROM beta WHERE id = 12")
	alphaEnd := defaultCatalogNextBase + smallTableRegionSize
	if !bytes.Equal(before[defaultCatalogNextBase:alphaEnd], client.ram[defaultCatalogNextBase:alphaEnd]) {
		t.Fatal("changes to other tables modified alpha's region")
	}
}

func TestCreateTableRespectsNodeCapacity(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact_fit_%t", exact), func(t *testing.T) {
			client := newMemTxnClient()
			limit := uint64(defaultCatalogNextBase + 2*smallTableRegionSize)
			if !exact {
				limit--
			}
			e := newRegionTestEngine(t, client, smallTableRegionSize, limit)
			execOK(t, e, "CREATE TABLE alpha")
			if exact {
				execOK(t, e, "CREATE TABLE beta")
			}
			before := bytes.Clone(client.ram)
			if _, err := e.Exec(t.Context(), "CREATE TABLE overflow"); !errors.Is(err, ErrStorageFull) {
				t.Fatalf("create beyond node limit: %v", err)
			}
			if !bytes.Equal(before, client.ram) {
				t.Fatal("rejected table creation changed storage")
			}
			e = newRegionTestEngine(t, client, smallTableRegionSize, limit)
			if _, err := e.Exec(t.Context(), "SELECT id, value FROM overflow"); !errors.Is(err, ErrTableNotFound) {
				t.Fatalf("rejected table persisted: %v", err)
			}
			if _, err := NewEngine(Config{TxnClient: client, MemorySize: defaultCatalogNextBase + smallTableRegionSize - 1}); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("reopen with insufficient memory: %v", err)
			}
		})
	}
}

func TestEngineRejectsInvalidRegionConfig(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{MemorySize: testMemorySize + 1},
		{MemorySize: math.MaxUint64},
		{MemorySize: testMemorySize, TableRegionSize: minuetindex.MinRegionSize - 1},
		{MemorySize: testMemorySize, TableRegionSize: math.MaxUint64},
	} {
		cfg.TxnClient = newMemTxnClient()
		if _, err := NewEngine(cfg); err == nil {
			t.Fatalf("accepted invalid config %+v", cfg)
		}
	}
}

func TestCatalogRejectsInvalidStoredRegions(t *testing.T) {
	for _, field := range []string{"overlap", "size_overflow", "size_zero", "next_base", "version", "magic", "count"} {
		t.Run(field, func(t *testing.T) {
			client := newMemTxnClient()
			e := newRegionTestEngine(t, client, smallTableRegionSize, testMemorySize)
			execOK(t, e, "CREATE TABLE alpha")
			execOK(t, e, "CREATE TABLE beta")
			second := catalogHeaderSize + catalogRecordSize
			switch field {
			case "overlap":
				binary.BigEndian.PutUint64(client.ram[second+catalogRecordBaseAddrOffset:], defaultCatalogNextBase)
			case "size_overflow":
				binary.BigEndian.PutUint64(client.ram[second+catalogRecordRegionSizeOffset:], math.MaxUint64)
			case "size_zero":
				binary.BigEndian.PutUint64(client.ram[second+catalogRecordRegionSizeOffset:], 0)
			case "next_base":
				binary.BigEndian.PutUint64(client.ram[catalogNextBaseAddrOffset:], defaultCatalogNextBase)
			case "version":
				binary.BigEndian.PutUint32(client.ram[catalogVersionOffset:], catalogFormatVersion-1)
			case "magic":
				client.ram[0] ^= 0xff
			case "count":
				binary.BigEndian.PutUint32(client.ram[catalogTableCountOffset:], catalogMaxRecords+1)
			}
			before := bytes.Clone(client.ram)
			if _, err := NewEngine(Config{TxnClient: client, MemorySize: testMemorySize}); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("reopen with corrupt %s: %v", field, err)
			}
			if _, err := e.Exec(t.Context(), "CREATE TABLE gamma"); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("create with corrupt %s: %v", field, err)
			}
			if !bytes.Equal(before, client.ram) {
				t.Fatal("invalid catalog was overwritten")
			}
		})
	}
}

func TestEngineRejectsTableBeyondCatalogCapacity(t *testing.T) {
	ctx := t.Context()
	base := newMemTxnClient()
	client := &conflictInjectingClient{base: base}
	engine := newTestEngineWithClient(t, client)
	staleEngine := newTestEngineWithClient(t, client)
	names := []string{"one", "two", "three", "four", "five", "six"}
	for _, name := range names {
		if _, err := engine.Exec(ctx, "CREATE TABLE "+name); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	before, err := base.Read(ctx, 1, catalogAddr, catalogSize)
	if err != nil {
		t.Fatal(err)
	}
	attempts := client.calls.Load()
	// Both a current engine and one with an empty cached catalog must consult
	// the stored catalog and reject creation before attempting a transaction.
	for _, candidate := range []*Engine{engine, staleEngine} {
		if _, err := candidate.Exec(ctx, "CREATE TABLE seven"); !errors.Is(err, ErrCatalogFull) {
			t.Errorf("seventh create error = %v, want ErrCatalogFull", err)
		}
		if _, err := candidate.Exec(ctx, "SELECT id, value FROM seven"); !errors.Is(err, ErrTableNotFound) {
			t.Errorf("rejected table is queryable: %v", err)
		}
		if _, err := candidate.Exec(ctx, "CREATE TABLE six"); !errors.Is(err, ErrTableExists) {
			t.Errorf("duplicate create in full catalog = %v, want ErrTableExists", err)
		}
	}
	if got := client.calls.Load(); got != attempts {
		t.Errorf("rejected creation attempted %d transactions", got-attempts)
	}
	after, err := base.Read(ctx, 1, catalogAddr, catalogSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("rejected creation changed the stored catalog")
	}
	reopened := newTestEngineWithClient(t, base)
	for _, name := range names {
		if _, err := reopened.Exec(ctx, "SELECT id, value FROM "+name); err != nil {
			t.Errorf("reopen %s: %v", name, err)
		}
	}
	if _, err := reopened.Exec(ctx, "SELECT id, value FROM seven"); !errors.Is(err, ErrTableNotFound) {
		t.Errorf("rejected table survived reopening: %v", err)
	}
}

func FuzzDecodeCatalogNeverPanics(f *testing.F) {
	valid := encodeCatalog(catalogState{
		nextTableID:  2,
		nextBaseAddr: defaultCatalogNextBase + defaultTableRegionSize,
		tables: []catalogTableRecord{{
			tableID:       1,
			name:          "accounts",
			indexBaseAddr: defaultCatalogNextBase,
			regionSize:    defaultTableRegionSize,
		}},
	})
	oversizedCount := bytes.Clone(valid)
	binary.BigEndian.PutUint32(oversizedCount[catalogTableCountOffset:], ^uint32(0))
	f.Add([]byte{}, uint64(0))
	f.Add(valid, uint64(testMemorySize))
	f.Add(oversizedCount, ^uint64(0))
	f.Fuzz(func(t *testing.T, raw []byte, memorySize uint64) {
		catalog, err := decodeCatalog(raw, memorySize)
		if err != nil {
			return
		}
		if len(catalog.tables) > catalogMaxRecords {
			t.Fatalf("decoded %d tables from a catalog with capacity %d", len(catalog.tables), catalogMaxRecords)
		}
		end := uint64(defaultCatalogNextBase)
		for _, rec := range catalog.tables {
			if rec.indexBaseAddr < end || rec.indexBaseAddr > memorySize || rec.regionSize > memorySize-rec.indexBaseAddr {
				t.Fatalf("accepted overlapping or out-of-bounds region: %+v", rec)
			}
			end = rec.indexBaseAddr + rec.regionSize
		}
		if catalog.nextBaseAddr != end || end > memorySize {
			t.Fatal("next table allocation is outside available memory")
		}
	})
}
