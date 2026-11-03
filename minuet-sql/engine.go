package minuetsql

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	minuetindex "github.com/jbancroft/ConcertumDemo/minuet-index"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const (
	defaultHostID          = 1
	defaultMaxRetries      = 8
	defaultRetryBackoff    = time.Millisecond
	defaultTableRegionSize = 64 << 10 // 64 KiB, including index metadata.
)

var (
	ErrBusy           = errors.New("minuet-sql: retry budget exhausted")
	ErrCatalogFull    = errors.New("minuet-sql: catalog full")
	ErrDuplicateKey   = errors.New("minuet-sql: duplicate key")
	ErrInvalidCatalog = errors.New("minuet-sql: invalid catalog")
	ErrRowNotFound    = errors.New("minuet-sql: row not found")
	ErrStorageFull    = errors.New("minuet-sql: no space for another table region")
	ErrTableExists    = errors.New("minuet-sql: table already exists")
	ErrTableFull      = errors.New("minuet-sql: table region full")
	ErrTableNotFound  = errors.New("minuet-sql: table not found")
	ErrUnsupportedSQL = errors.New("minuet-sql: unsupported SQL")
)

type Row struct {
	ID    uint64
	Value uint64
}

type QueryResult struct {
	Rows []Row
}

type Config struct {
	HostId          uint32
	MemorySize      uint64 // Required upper bound on addresses used by SQL; must fit the node.
	TableRegionSize uint64 // Bytes reserved for each new table; defaults to 64 KiB.
	TxnClient       sinfoniaclient.TxnClient
	MaxRetries      int
	RetryBackoff    time.Duration
}

type Engine struct {
	cfg    Config
	tables map[string]*table
}

type table struct {
	index    *minuetindex.BTree
	keyLocks keyLocker
}

type keyLocker struct {
	mu    sync.Mutex
	locks map[uint64]*keyLock
}

type keyLock struct {
	held chan struct{} // Capacity one lets acquisition wait on context cancellation.
	refs int           // Holders and waiters; protected by keyLocker.mu.
}

func (k *keyLocker) withLock(ctx context.Context, key uint64, fn func() (QueryResult, error)) (QueryResult, error) {
	l := k.lockFor(key)
	defer func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		l.refs--
		if l.refs == 0 {
			delete(k.locks, key)
		}
	}()
	select {
	case l.held <- struct{}{}:
		defer func() { <-l.held }()
	case <-ctx.Done():
		return QueryResult{}, ctx.Err()
	}
	// Acquisition and cancellation can both be ready in the select above.
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	return fn()
}

func (k *keyLocker) lockFor(key uint64) *keyLock {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = map[uint64]*keyLock{}
	}
	l, ok := k.locks[key]
	if !ok {
		l = &keyLock{held: make(chan struct{}, 1)}
		k.locks[key] = l
	}
	l.refs++
	return l
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.TxnClient == nil {
		return nil, errors.New("minuet-sql: txn client is required")
	}
	if cfg.MemorySize < defaultCatalogNextBase {
		return nil, errors.New("minuet-sql: memory size must include the catalog reservation")
	}
	cfg.HostId = cmp.Or(cfg.HostId, defaultHostID)
	cfg.TableRegionSize = cmp.Or(cfg.TableRegionSize, defaultTableRegionSize)
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultRetryBackoff
	}
	e := &Engine{cfg: cfg, tables: map[string]*table{}}
	// Validate the default for future tables, even if the catalog is empty.
	if _, err := e.openTable(catalogTableRecord{indexBaseAddr: defaultCatalogNextBase, regionSize: cfg.TableRegionSize}); err != nil {
		return nil, err
	}
	if _, err := cfg.TxnClient.Read(context.Background(), cfg.HostId, cfg.MemorySize-1, 1); err != nil {
		return nil, fmt.Errorf("minuet-sql: memory limit is not readable: %w", err)
	}
	if err := e.loadCatalog(context.Background()); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) Exec(ctx context.Context, sql string) (QueryResult, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return QueryResult{}, err
	}

	switch n := stmt.(type) {
	case createTableStmt:
		if err := e.createTable(ctx, n.name); err != nil {
			return QueryResult{}, err
		}
		return QueryResult{}, nil
	case insertStmt:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		return t.keyLocks.withLock(ctx, n.row.ID, func() (QueryResult, error) {
			_, found, err := t.index.Search(ctx, n.row.ID)
			if err != nil {
				return QueryResult{}, mapIndexError(err)
			}
			if found {
				return QueryResult{}, ErrDuplicateKey
			}
			if err := t.index.Insert(ctx, n.row.ID, n.row.Value); err != nil {
				return QueryResult{}, mapIndexError(err)
			}
			return QueryResult{}, nil
		})
	case deleteStmt:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		return t.keyLocks.withLock(ctx, n.id, func() (QueryResult, error) {
			_, found, err := t.index.Search(ctx, n.id)
			if err != nil {
				return QueryResult{}, mapIndexError(err)
			}
			if !found {
				return QueryResult{}, ErrRowNotFound
			}
			if err := t.index.Delete(ctx, n.id); err != nil {
				return QueryResult{}, mapIndexError(err)
			}
			return QueryResult{}, nil
		})
	case selectStmt:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		if n.byID {
			value, found, err := t.index.Search(ctx, n.id)
			if err != nil {
				return QueryResult{}, mapIndexError(err)
			}
			if !found {
				return QueryResult{Rows: nil}, nil
			}
			return QueryResult{Rows: []Row{{ID: n.id, Value: value}}}, nil
		}
		it, err := t.index.RangeScan(ctx, 0, math.MaxUint64)
		if err != nil {
			return QueryResult{}, mapIndexError(err)
		}
		out := make([]Row, 0)
		for {
			entry, ok := it.Next()
			if !ok {
				break
			}
			out = append(out, Row{ID: entry.Key, Value: entry.Value})
		}
		slices.SortFunc(out, func(a, b Row) int { return cmp.Compare(a.ID, b.ID) })
		return QueryResult{Rows: out}, nil
	default:
		return QueryResult{}, fmt.Errorf("%w: %T", ErrUnsupportedSQL, stmt)
	}
}

func (e *Engine) loadCatalog(ctx context.Context) error {
	raw, err := e.cfg.TxnClient.Read(ctx, e.cfg.HostId, catalogAddr, catalogSize)
	if err != nil {
		return err
	}
	catalog, err := decodeCatalog(raw, e.cfg.MemorySize)
	if err != nil {
		return err
	}
	tables := make(map[string]*table, len(catalog.tables))
	for _, rec := range catalog.tables {
		t, err := e.openTable(rec)
		if err != nil {
			return err
		}
		tables[rec.name] = t
	}
	e.tables = tables
	return nil
}

func (e *Engine) createTable(ctx context.Context, name string) error {
	for range e.cfg.MaxRetries {
		raw, err := e.cfg.TxnClient.Read(ctx, e.cfg.HostId, catalogAddr, catalogSize)
		if err != nil {
			return err
		}
		catalog, err := decodeCatalog(raw, e.cfg.MemorySize)
		if err != nil {
			return err
		}
		if _, exists := catalog.tableByName(name); exists {
			return ErrTableExists
		}
		if len(catalog.tables) >= catalogMaxRecords {
			return ErrCatalogFull
		}
		if e.cfg.TableRegionSize > e.cfg.MemorySize-catalog.nextBaseAddr {
			return ErrStorageFull
		}
		rec := catalog.newTableRecord(name, e.cfg.TableRegionSize)
		t, err := e.openTable(rec)
		if err != nil {
			return err
		}
		next := catalog.cloneWithTable(rec)
		compares := []sinfoniaclient.CompareItem{
			{HostId: e.cfg.HostId, Address: catalogAddr, Data: raw},
			{HostId: e.cfg.HostId, Address: rec.indexBaseAddr, Data: make([]byte, minuetIndexSuperblockSize)},
		}
		writes := []sinfoniaclient.WriteItem{
			{HostId: e.cfg.HostId, Address: catalogAddr, Data: encodeCatalog(next)},
			{HostId: e.cfg.HostId, Address: rec.indexBaseAddr, Data: make([]byte, minuetIndexSuperblockSize)},
		}
		result := e.cfg.TxnClient.ExecuteAndCommit(ctx, compares, nil, writes)
		switch result.Status {
		case sinfoniaclient.StatusCommitted:
			// Publish the table below only after a confirmed commit.
		case sinfoniaclient.StatusAborted:
			timer := time.NewTimer(e.cfg.RetryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		default:
			return result.OutcomeError()
		}
		e.tables[name] = t
		return nil
	}
	return ErrBusy
}

func (e *Engine) openTable(rec catalogTableRecord) (*table, error) {
	index, err := minuetindex.NewBTree(e.cfg.TxnClient, minuetindex.Config{
		HostId: e.cfg.HostId, BaseAddress: rec.indexBaseAddr, RegionSize: rec.regionSize,
		MaxRetries: e.cfg.MaxRetries, RetryBackoff: e.cfg.RetryBackoff,
	})
	if err != nil {
		return nil, err
	}
	return &table{index: index}, nil
}

func mapIndexError(err error) error {
	if errors.Is(err, minuetindex.ErrBusy) {
		return ErrBusy
	}
	if errors.Is(err, minuetindex.ErrRegionFull) {
		return ErrTableFull
	}
	return err
}

func normalizeName(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}
