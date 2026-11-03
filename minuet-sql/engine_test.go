package minuetsql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// Statement execution, concurrent access, retries, and uncertain transaction outcomes.

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

func (m *memTxnClient) ExecuteAndCommit(_ context.Context, compares []sinfoniaclient.CompareItem, _ []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
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

func (c *conflictInjectingClient) ExecuteAndCommit(ctx context.Context, compares []sinfoniaclient.CompareItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	_ = reads
	call := atomic.AddInt32(&c.calls, 1)
	if call <= atomic.LoadInt32(&c.rejectFirstN) {
		return sinfoniaclient.ExecResult{Committed: false, Status: sinfoniaclient.StatusAborted, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
	}
	return c.base.ExecuteAndCommit(ctx, compares, reads, writes)
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

// Only the first transaction is uncertain; an accidental retry would succeed.
type unknownOutcomeClient struct {
	sinfoniaclient.TxnClient
	applyWrites bool
	attempts    int
	failure     error
}

func (c *unknownOutcomeClient) ExecuteAndCommit(ctx context.Context, compares []sinfoniaclient.CompareItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	c.attempts++
	if c.attempts > 1 {
		return c.TxnClient.ExecuteAndCommit(ctx, compares, reads, writes)
	}
	if c.applyWrites {
		result := c.TxnClient.ExecuteAndCommit(ctx, compares, reads, writes)
		if result.Status != sinfoniaclient.StatusCommitted {
			return result
		}
	}
	return sinfoniaclient.ExecResult{Status: sinfoniaclient.StatusUnknown, UnknownOutcome: true, Err: c.failure}
}

func TestEngineCreateInsertSelectDelete(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)

	statements := []string{
		"CREATE TABLE accounts",
		"insert into accounts values (1, 100)",
		"INSERT INTO accounts VALUES (2, 250)",
	}
	for _, sql := range statements {
		if _, err := e.Exec(ctx, sql); err != nil {
			t.Fatalf("Exec(%q): %v", sql, err)
		}
	}

	all, err := e.Exec(ctx, "SELECT id, value FROM accounts")
	if err != nil {
		t.Fatalf("select all: %v", err)
	}
	if !reflect.DeepEqual(all.Rows, []Row{{ID: 1, Value: 100}, {ID: 2, Value: 250}}) {
		t.Fatalf("select all rows=%#v", all.Rows)
	}

	one, err := e.Exec(ctx, "SELECT id, value FROM accounts WHERE id = 2")
	if err != nil {
		t.Fatalf("select by id: %v", err)
	}
	if !reflect.DeepEqual(one.Rows, []Row{{ID: 2, Value: 250}}) {
		t.Fatalf("select by id rows=%#v", one.Rows)
	}

	missing, err := e.Exec(ctx, "SELECT id, value FROM accounts WHERE id = 99")
	if err != nil {
		t.Fatalf("select missing id: %v", err)
	}
	if len(missing.Rows) != 0 {
		t.Fatalf("select missing rows=%#v, want empty", missing.Rows)
	}

	if _, err := e.Exec(ctx, "DELETE FROM accounts WHERE id = 1"); err != nil {
		t.Fatalf("delete existing row: %v", err)
	}
	afterDelete, err := e.Exec(ctx, "SELECT id, value FROM accounts")
	if err != nil {
		t.Fatalf("select after delete: %v", err)
	}
	if !reflect.DeepEqual(afterDelete.Rows, []Row{{ID: 2, Value: 250}}) {
		t.Fatalf("unexpected rows after delete: %#v", afterDelete.Rows)
	}
	if _, err := e.Exec(ctx, "DELETE FROM accounts WHERE id = 99"); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("delete missing err=%v, want ErrRowNotFound", err)
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

func TestEngineMissingDeletesReleaseKeyLocks(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	if _, err := e.Exec(ctx, "CREATE TABLE t"); err != nil {
		t.Fatal(err)
	}
	for id := 0; id < 256; id++ {
		if _, err := e.Exec(ctx, fmt.Sprintf("DELETE FROM t WHERE id = %d", id)); !errors.Is(err, ErrRowNotFound) {
			t.Fatalf("delete id %d: %v, want ErrRowNotFound", id, err)
		}
	}
	if retained := len(e.tables["t"].keyLocks.locks); retained != 0 {
		t.Fatalf("retained %d key locks after missing deletes", retained)
	}
}

func TestKeyLockerConcurrentAccess(t *testing.T) {
	const (
		iterations = 100
		workers    = 16
	)
	var locker keyLocker
	var wg sync.WaitGroup
	start := make(chan struct{})
	count := 0
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				_, _ = locker.withLock(7, func() (QueryResult, error) {
					previous := count
					// Let waiters contend while the callback owns the lock.
					runtime.Gosched()
					count = previous + 1
					return QueryResult{}, nil
				})
			}
		}()
	}
	close(start)
	wg.Wait()
	if count != workers*iterations {
		t.Fatalf("serialized increments = %d, want %d", count, workers*iterations)
	}
	if retained := len(locker.locks); retained != 0 {
		t.Fatalf("retained %d key locks after concurrent calls", retained)
	}
}

func TestEngineRejectsTransactionBoundariesWithoutGroupingStatements(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	for _, sql := range []string{
		"CREATE TABLE generated_parents",
		"CREATE TABLE generated_children",
		"INSERT INTO generated_parents VALUES (7, 70)",
		"INSERT INTO generated_children VALUES (1, 7)",
	} {
		if _, err := e.Exec(ctx, sql); err != nil {
			t.Fatalf("Exec(%q): %v", sql, err)
		}
	}

	for _, sql := range []string{
		"SELECT id, value FROM generated_parents JOIN generated_children ON generated_parents.id = generated_children.value",
		"UPDATE generated_parents SET value = 71 WHERE id = 7",
		"BEGIN",
	} {
		if _, err := e.Exec(ctx, sql); !errors.Is(err, ErrUnsupportedSQL) {
			t.Fatalf("Exec(%q) err=%v, want ErrUnsupportedSQL", sql, err)
		}
	}
	if _, err := e.Exec(ctx, "DELETE FROM generated_children WHERE id = 1"); err != nil {
		t.Fatalf("delete child: %v", err)
	}
	for _, boundary := range []string{"ROLLBACK", "COMMIT"} {
		if _, err := e.Exec(ctx, boundary); !errors.Is(err, ErrUnsupportedSQL) {
			t.Fatalf("Exec(%q) err=%v, want ErrUnsupportedSQL", boundary, err)
		}
	}

	parents, err := e.Exec(ctx, "SELECT id, value FROM generated_parents")
	if err != nil || !reflect.DeepEqual(parents.Rows, []Row{{ID: 7, Value: 70}}) {
		t.Fatalf("parent rows=%#v err=%v", parents.Rows, err)
	}
	children, err := e.Exec(ctx, "SELECT id, value FROM generated_children")
	if err != nil || len(children.Rows) != 0 {
		t.Fatalf("child rows=%#v err=%v", children.Rows, err)
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

func TestCreateTableRetriesConfirmedAbort(t *testing.T) {
	ctx := context.Background()
	base := newMemTxnClient()
	client := &conflictInjectingClient{base: base, rejectFirstN: 2}
	engine := newTestEngineWithClient(t, client)
	if _, err := engine.Exec(ctx, "CREATE TABLE scores"); err != nil {
		t.Fatal(err)
	}
	if attempts := atomic.LoadInt32(&client.calls); attempts != 3 {
		t.Fatalf("commit attempts = %d, want 3", attempts)
	}
	reopened := newTestEngineWithClient(t, base)
	if _, err := reopened.Exec(ctx, "SELECT id, value FROM scores"); err != nil {
		t.Fatalf("created table was not persisted: %v", err)
	}
}

func TestCreateTableUnknownOutcomeIsNotRetried(t *testing.T) {
	for _, test := range []struct {
		name        string
		applyWrites bool
	}{
		{name: "without commit"},
		{name: "lost commit reply", applyWrites: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := newMemTxnClient()
			client := &unknownOutcomeClient{TxnClient: base, applyWrites: test.applyWrites, failure: errors.New("lost reply")}
			engine := newTestEngineWithClient(t, client)
			_, err := engine.Exec(ctx, "CREATE TABLE scores")
			if !errors.Is(err, sinfoniaclient.ErrUnknownOutcome) {
				t.Errorf("create error = %v, want ErrUnknownOutcome", err)
			}
			if !errors.Is(err, client.failure) {
				t.Fatalf("lost original failure: %v", err)
			}
			if client.attempts != 1 {
				t.Errorf("commit attempts = %d, want 1", client.attempts)
			}
			// An uncertain creation must not publish a table in this engine's cache.
			if _, err := engine.Exec(ctx, "SELECT id, value FROM scores"); !errors.Is(err, ErrTableNotFound) {
				t.Errorf("select in original engine = %v, want ErrTableNotFound", err)
			}
			// Inspect the actual stored catalog with a new engine: either outcome
			// is possible even though the original caller received the same error.
			reopened := newTestEngineWithClient(t, base)
			result, err := reopened.Exec(ctx, "SELECT id, value FROM scores")
			if test.applyWrites {
				if err != nil || len(result.Rows) != 0 {
					t.Fatalf("reopened table: rows = %v, error = %v", result.Rows, err)
				}
			} else if !errors.Is(err, ErrTableNotFound) {
				t.Fatalf("reopened table error = %v, want ErrTableNotFound", err)
			}
		})
	}
}

func TestRowMutationPropagatesUnknownOutcome(t *testing.T) {
	for _, test := range []struct {
		name        string
		delete      bool
		applyWrites bool
	}{
		{name: "insert without commit"},
		{name: "insert with lost commit reply", applyWrites: true},
		{name: "delete without commit", delete: true},
		{name: "delete with lost commit reply", delete: true, applyWrites: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := newMemTxnClient()
			seed := newTestEngineWithClient(t, base)
			if _, err := seed.Exec(ctx, "CREATE TABLE scores"); err != nil {
				t.Fatal(err)
			}
			if test.delete {
				if _, err := seed.Exec(ctx, "INSERT INTO scores VALUES (7, 70)"); err != nil {
					t.Fatal(err)
				}
			}
			client := &unknownOutcomeClient{TxnClient: base, applyWrites: test.applyWrites, failure: errors.New("lost reply")}
			engine := newTestEngineWithClient(t, client)
			statement := "INSERT INTO scores VALUES (7, 70)"
			if test.delete {
				statement = "DELETE FROM scores WHERE id = 7"
			}
			if _, err := engine.Exec(ctx, statement); !errors.Is(err, sinfoniaclient.ErrUnknownOutcome) {
				t.Errorf("mutation error = %v, want ErrUnknownOutcome", err)
			}
			if client.attempts != 1 {
				t.Errorf("commit attempts = %d, want 1", client.attempts)
			}
			wantRows := 0
			if test.applyWrites != test.delete {
				wantRows = 1
			}
			result, err := seed.Exec(ctx, "SELECT id, value FROM scores")
			if err != nil || len(result.Rows) != wantRows {
				t.Fatalf("rows = %v, error = %v; want %d rows", result.Rows, err, wantRows)
			}
			if wantRows == 1 && result.Rows[0] != (Row{ID: 7, Value: 70}) {
				t.Fatalf("unexpected row: %v", result.Rows[0])
			}
		})
	}
}
