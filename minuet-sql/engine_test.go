package minuetsql

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

type memTxnClient struct {
	mu  sync.Mutex
	ram []byte
}

const testMemorySize = 1 << 22

func newMemTxnClient() *memTxnClient { return &memTxnClient{ram: make([]byte, testMemorySize)} }

func (m *memTxnClient) Read(_ context.Context, _ uint32, addr uint64, length uint32) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if addr > uint64(len(m.ram)) || uint64(length) > uint64(len(m.ram))-addr {
		return nil, errors.New("memory read out of bounds")
	}
	out := make([]byte, length)
	copy(out, m.ram[int(addr):int(addr)+int(length)])
	return out, nil
}

func (m *memTxnClient) Write(_ context.Context, _ uint32, addr uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy(m.ram[int(addr):int(addr)+len(data)], data)
	return nil
}

func (m *memTxnClient) ExecuteAndCommit(_ context.Context, _ string, compares []sinfoniaclient.CompareItem, _ []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range compares {
		if string(m.ram[int(c.Address):int(c.Address)+len(c.Data)]) != string(c.Data) {
			return sinfoniaclient.ExecResult{Committed: false, Status: sinfoniaclient.StatusAborted, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
		}
	}
	for _, w := range writes {
		copy(m.ram[int(w.Address):int(w.Address)+len(w.Data)], w.Data)
	}
	return sinfoniaclient.ExecResult{Committed: true, Status: sinfoniaclient.StatusCommitted, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
}

type conflictInjectingClient struct {
	base         *memTxnClient
	rejectFirstN int32
	calls        int32
}

func (c *conflictInjectingClient) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return c.base.Read(ctx, hostId, addr, length)
}

func (c *conflictInjectingClient) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return c.base.Write(ctx, hostId, addr, data)
}

func (c *conflictInjectingClient) ExecuteAndCommit(ctx context.Context, class string, compares []sinfoniaclient.CompareItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	_ = class
	_ = reads
	call := atomic.AddInt32(&c.calls, 1)
	if call <= atomic.LoadInt32(&c.rejectFirstN) {
		return sinfoniaclient.ExecResult{Committed: false, Status: sinfoniaclient.StatusAborted, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
	}
	return c.base.ExecuteAndCommit(ctx, class, compares, reads, writes)
}

func newTestEngineWithClient(t *testing.T, client sinfoniaclient.TxnClient) *Engine {
	t.Helper()
	e, err := NewEngine(Config{
		HostId:       1,
		TxnClient:    client,
		MemorySize:   testMemorySize,
		RetryBackoff: time.Microsecond,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return e
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return newTestEngineWithClient(t, newMemTxnClient())
}

func TestEngineCreateInsertSelectDeleteEndToEnd(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)

	if _, err := e.Exec(ctx, "CREATE TABLE accounts"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := e.Exec(ctx, "INSERT INTO accounts VALUES (1, 100)"); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := e.Exec(ctx, "INSERT INTO accounts VALUES (2, 250)"); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	all, err := e.Exec(ctx, "SELECT id, value FROM accounts")
	if err != nil {
		t.Fatalf("select all: %v", err)
	}
	if len(all.Rows) != 2 || all.Rows[0].ID != 1 || all.Rows[1].ID != 2 {
		t.Fatalf("unexpected select all rows: %#v", all.Rows)
	}

	one, err := e.Exec(ctx, "SELECT id, value FROM accounts WHERE id = 2")
	if err != nil {
		t.Fatalf("select by id: %v", err)
	}
	if len(one.Rows) != 1 || one.Rows[0].Value != 250 {
		t.Fatalf("unexpected select by id rows: %#v", one.Rows)
	}

	if _, err := e.Exec(ctx, "DELETE FROM accounts WHERE id = 1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	afterDelete, err := e.Exec(ctx, "SELECT id, value FROM accounts")
	if err != nil {
		t.Fatalf("select after delete: %v", err)
	}
	if len(afterDelete.Rows) != 1 || afterDelete.Rows[0].ID != 2 {
		t.Fatalf("unexpected rows after delete: %#v", afterDelete.Rows)
	}
}

func TestEngineRejectsDuplicatePrimaryKey(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	_, _ = e.Exec(ctx, "CREATE TABLE t")
	_, _ = e.Exec(ctx, "INSERT INTO t VALUES (7, 70)")
	_, err := e.Exec(ctx, "INSERT INTO t VALUES (7, 71)")
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate insert err=%v want=%v", err, ErrDuplicateKey)
	}
}

func TestEngineRejectsTableBeyondCatalogCapacity(t *testing.T) {
	ctx := context.Background()
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
	attempts := atomic.LoadInt32(&client.calls)
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
	if got := atomic.LoadInt32(&client.calls); got != attempts {
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

func TestEngineConcurrentInsertSamePrimaryKeyDeterministic(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	if _, err := e.Exec(ctx, "CREATE TABLE t"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := e.Exec(ctx, "INSERT INTO t VALUES (9, 90)")
		errs <- err
	}()
	go func() {
		<-start
		_, err := e.Exec(ctx, "INSERT INTO t VALUES (9, 91)")
		errs <- err
	}()
	close(start)
	errA, errB := <-errs, <-errs
	successes := 0
	dups := 0
	for _, err := range []error{errA, errB} {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrDuplicateKey):
			dups++
		default:
			t.Fatalf("unexpected concurrent insert error: %v", err)
		}
	}
	if successes != 1 || dups != 1 {
		t.Fatalf("concurrent insert outcomes want (1 success,1 duplicate) got success=%d duplicate=%d", successes, dups)
	}
	res, err := e.Exec(ctx, "SELECT id, value FROM t WHERE id = 9")
	if err != nil {
		t.Fatalf("select inserted row: %v", err)
	}
	if len(res.Rows) != 1 || (res.Rows[0].Value != 90 && res.Rows[0].Value != 91) {
		t.Fatalf("unexpected winner row: %#v", res.Rows)
	}
}

func TestEngineConcurrentDeleteSelectSamePrimaryKey(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	if _, err := e.Exec(ctx, "CREATE TABLE t"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := e.Exec(ctx, "INSERT INTO t VALUES (4, 40)"); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	start := make(chan struct{})
	deleteErr := make(chan error, 1)
	selectResult := make(chan QueryResult, 1)
	selectErr := make(chan error, 1)
	go func() {
		<-start
		_, err := e.Exec(ctx, "DELETE FROM t WHERE id = 4")
		deleteErr <- err
	}()
	go func() {
		<-start
		res, err := e.Exec(ctx, "SELECT id, value FROM t WHERE id = 4")
		selectResult <- res
		selectErr <- err
	}()
	close(start)
	if err := <-deleteErr; err != nil {
		t.Fatalf("delete err=%v", err)
	}
	res := <-selectResult
	if err := <-selectErr; err != nil {
		t.Fatalf("select err=%v", err)
	}
	if len(res.Rows) == 1 && res.Rows[0] != (Row{ID: 4, Value: 40}) {
		t.Fatalf("select returned unexpected row: %#v", res.Rows)
	}
	if len(res.Rows) > 1 {
		t.Fatalf("select returned multiple rows: %#v", res.Rows)
	}
	after, err := e.Exec(ctx, "SELECT id, value FROM t WHERE id = 4")
	if err != nil {
		t.Fatalf("select after delete: %v", err)
	}
	if len(after.Rows) != 0 {
		t.Fatalf("row should be deleted, got: %#v", after.Rows)
	}
	if _, err := e.Exec(ctx, "DELETE FROM t WHERE id = 4"); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("second delete err=%v want=%v", err, ErrRowNotFound)
	}
}

func TestEngineConflictRetryBudgetMapsToErrBusy(t *testing.T) {
	ctx := context.Background()
	client := &conflictInjectingClient{base: newMemTxnClient(), rejectFirstN: 0}
	e, err := NewEngine(Config{
		HostId:       1,
		TxnClient:    client,
		MaxRetries:   2,
		MemorySize:   testMemorySize,
		RetryBackoff: time.Microsecond,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if _, err := e.Exec(ctx, "CREATE TABLE t"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	atomic.StoreInt32(&client.calls, 0)
	atomic.StoreInt32(&client.rejectFirstN, 100)
	_, err = e.Exec(ctx, "INSERT INTO t VALUES (12, 120)")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("insert err=%v want=%v", err, ErrBusy)
	}
}
