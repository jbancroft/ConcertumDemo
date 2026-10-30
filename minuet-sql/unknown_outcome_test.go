package minuetsql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// Only the first transaction is uncertain; an accidental retry would succeed.
type unknownOutcomeClient struct {
	sinfoniaclient.TxnClient
	applyWrites bool
	attempts    int
	handle      sinfoniaclient.RecoveryHandle
}

func (c *unknownOutcomeClient) ExecuteAndCommit(ctx context.Context, class string, compares []sinfoniaclient.CompareItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	c.attempts++
	if c.attempts > 1 {
		return c.TxnClient.ExecuteAndCommit(ctx, class, compares, reads, writes)
	}
	if c.applyWrites {
		result := c.TxnClient.ExecuteAndCommit(ctx, class, compares, reads, writes)
		if result.Status != sinfoniaclient.StatusCommitted {
			return result
		}
	}
	return sinfoniaclient.ExecResult{Status: sinfoniaclient.StatusUnknown, UnknownOutcome: true, Recovery: &c.handle}
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
			client := &unknownOutcomeClient{TxnClient: base, applyWrites: test.applyWrites}
			engine := newTestEngineWithClient(t, client)
			_, err := engine.Exec(ctx, "CREATE TABLE scores")
			if !errors.Is(err, sinfoniaclient.ErrUnknownOutcome) {
				t.Errorf("create error = %v, want ErrUnknownOutcome", err)
			}
			var uncertain *sinfoniaclient.UnknownOutcomeError
			if !errors.As(err, &uncertain) || uncertain.Handle != &client.handle {
				t.Fatalf("lost recovery handle: %v", err)
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
			client := &unknownOutcomeClient{TxnClient: base, applyWrites: test.applyWrites}
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
