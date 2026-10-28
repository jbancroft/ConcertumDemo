package minuetsql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	minuetindex "github.com/jbancroft/ConcertumDemo/minuet-index"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

var (
	ErrTableExists    = errors.New("minuet-sql: table already exists")
	ErrTableNotFound  = errors.New("minuet-sql: table not found")
	ErrRowNotFound    = errors.New("minuet-sql: row not found")
	ErrDuplicateKey   = errors.New("minuet-sql: duplicate key")
	ErrUnsupportedSQL = errors.New("minuet-sql: unsupported SQL")
	ErrBusy           = errors.New("minuet-sql: retry budget exhausted")
)

type Row struct {
	ID    uint64
	Value uint64
}

type QueryResult struct {
	Rows []Row
}

type BTreeFactory func(hostId uint32, baseAddr uint64) *minuetindex.BTree

type QuotaEnforcer interface {
	CheckAndConsumeQuota(ctx context.Context, resource string, delta int64) error
}

type Config struct {
	HostId       uint32
	NextBaseAddr uint64
	TxnClient    sinfoniaclient.TxnClient
	BTreeFactory BTreeFactory
	MaxRetries   int
	RetryBackoff time.Duration
	Quota        QuotaEnforcer
}

type Engine struct {
	cfg    Config
	tables map[string]*table
}

type table struct {
	name          string
	tableID       uint64
	schemaVersion uint32
	indexBaseAddr uint64
	index         *minuetindex.BTree
	keyLocks      keyLocker
}

type keyLocker struct {
	mu    sync.Mutex
	locks map[uint64]*sync.Mutex
}

func (k *keyLocker) withLock(key uint64, fn func() (QueryResult, error)) (QueryResult, error) {
	l := k.lockFor(key)
	l.Lock()
	defer l.Unlock()
	return fn()
}

func (k *keyLocker) lockFor(key uint64) *sync.Mutex {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = map[uint64]*sync.Mutex{}
	}
	l, ok := k.locks[key]
	if !ok {
		l = &sync.Mutex{}
		k.locks[key] = l
	}
	return l
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.TxnClient == nil {
		return nil, errors.New("minuet-sql: txn client is required")
	}
	if cfg.BTreeFactory == nil {
		return nil, errors.New("minuet-sql: index factory is required")
	}
	if cfg.HostId == 0 {
		cfg.HostId = 1
	}
	if cfg.NextBaseAddr == 0 {
		cfg.NextBaseAddr = defaultCatalogNextBase
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 8
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Millisecond
	}
	e := &Engine{cfg: cfg, tables: map[string]*table{}}
	if err := e.loadCatalog(context.Background()); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) Exec(ctx context.Context, sql string) (QueryResult, error) {
	if e.cfg.Quota != nil {
		if err := e.cfg.Quota.CheckAndConsumeQuota(ctx, "transactions", 1); err != nil {
			return QueryResult{}, err
		}
	}
	stmt, err := Parse(sql)
	if err != nil {
		return QueryResult{}, err
	}
	plan, err := Plan(stmt)
	if err != nil {
		return QueryResult{}, err
	}
	return e.execute(ctx, plan)
}

func (e *Engine) execute(ctx context.Context, p planNode) (QueryResult, error) {
	switch n := p.(type) {
	case createTablePlan:
		if err := e.createTable(ctx, n.name); err != nil {
			return QueryResult{}, err
		}
		return QueryResult{}, nil
	case insertPlan:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		return t.keyLocks.withLock(n.row.ID, func() (QueryResult, error) {
			_, found, err := t.index.Search(ctx, n.row.ID)
			if err != nil {
				return QueryResult{}, mapBusy(err)
			}
			if found {
				return QueryResult{}, ErrDuplicateKey
			}
			if err := t.index.Insert(ctx, n.row.ID, n.row.Value); err != nil {
				return QueryResult{}, mapBusy(err)
			}
			return QueryResult{}, nil
		})
	case deletePlan:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		return t.keyLocks.withLock(n.id, func() (QueryResult, error) {
			_, found, err := t.index.Search(ctx, n.id)
			if err != nil {
				return QueryResult{}, mapBusy(err)
			}
			if !found {
				return QueryResult{}, ErrRowNotFound
			}
			if err := t.index.Delete(ctx, n.id); err != nil {
				return QueryResult{}, mapBusy(err)
			}
			return QueryResult{}, nil
		})
	case selectPlan:
		t, ok := e.tables[n.table]
		if !ok {
			return QueryResult{}, ErrTableNotFound
		}
		if n.byID {
			value, found, err := t.index.Search(ctx, n.id)
			if err != nil {
				return QueryResult{}, mapBusy(err)
			}
			if !found {
				return QueryResult{Rows: nil}, nil
			}
			return QueryResult{Rows: []Row{{ID: n.id, Value: value}}}, nil
		}
		it, err := t.index.RangeScan(ctx, 0, math.MaxUint64)
		if err != nil {
			return QueryResult{}, mapBusy(err)
		}
		out := make([]Row, 0)
		for {
			entry, ok := it.Next()
			if !ok {
				break
			}
			out = append(out, Row{ID: entry.Key, Value: entry.Value})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return QueryResult{Rows: out}, nil
	default:
		return QueryResult{}, fmt.Errorf("%w: %T", ErrUnsupportedSQL, p)
	}
}

func (e *Engine) loadCatalog(ctx context.Context) error {
	raw, err := e.cfg.TxnClient.Read(ctx, e.cfg.HostId, catalogAddr, catalogSize)
	if err != nil {
		return err
	}
	catalog := decodeCatalog(raw, e.cfg.NextBaseAddr)
	tables := make(map[string]*table, len(catalog.tables))
	for _, rec := range catalog.tables {
		tables[rec.name] = &table{
			name:          rec.name,
			tableID:       rec.tableID,
			schemaVersion: rec.schemaVersion,
			indexBaseAddr: rec.indexBaseAddr,
			index:         e.cfg.BTreeFactory(e.cfg.HostId, rec.indexBaseAddr),
			keyLocks:      keyLocker{},
		}
	}
	e.tables = tables
	return nil
}

func (e *Engine) createTable(ctx context.Context, name string) error {
	if e.cfg.Quota != nil {
		if err := e.cfg.Quota.CheckAndConsumeQuota(ctx, "sql_objects", 1); err != nil {
			return err
		}
	}
	for attempt := 0; attempt < e.cfg.MaxRetries; attempt++ {
		raw, err := e.cfg.TxnClient.Read(ctx, e.cfg.HostId, catalogAddr, catalogSize)
		if err != nil {
			return err
		}
		catalog := decodeCatalog(raw, e.cfg.NextBaseAddr)
		if _, exists := catalog.tableByName(name); exists {
			return ErrTableExists
		}
		rec := catalog.newTableRecord(name)
		next := catalog.cloneWithTable(rec)
		compares := []sinfoniaclient.CompareItem{
			{HostId: e.cfg.HostId, Address: catalogAddr, Data: raw},
			{HostId: e.cfg.HostId, Address: rec.indexBaseAddr, Data: make([]byte, minuetIndexSuperblockSize)},
		}
		writes := []sinfoniaclient.WriteItem{
			{HostId: e.cfg.HostId, Address: catalogAddr, Data: encodeCatalog(next)},
			{HostId: e.cfg.HostId, Address: rec.indexBaseAddr, Data: make([]byte, minuetIndexSuperblockSize)},
		}
		if !e.cfg.TxnClient.ExecuteAndCommit(ctx, "minuet-sql:create-table", compares, nil, writes).Committed {
			time.Sleep(e.cfg.RetryBackoff)
			continue
		}
		e.tables[name] = &table{
			name:          rec.name,
			tableID:       rec.tableID,
			schemaVersion: rec.schemaVersion,
			indexBaseAddr: rec.indexBaseAddr,
			index:         e.cfg.BTreeFactory(e.cfg.HostId, rec.indexBaseAddr),
			keyLocks:      keyLocker{},
		}
		return nil
	}
	return ErrBusy
}

func mapBusy(err error) error {
	if errors.Is(err, minuetindex.ErrBusy) {
		return ErrBusy
	}
	return err
}

func normalizeName(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}
