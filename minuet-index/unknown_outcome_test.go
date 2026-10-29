package minuetindex

import (
	"context"
	"errors"
	"testing"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// The first transaction has an unknown result, with or without applying writes.
// Further attempts would succeed, making an accidental retry observable.
type unknownOutcomeClient struct {
	sinfoniaclient.TxnClient
	applyWrites bool
	attempts    int
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
	return sinfoniaclient.ExecResult{Status: sinfoniaclient.StatusUnknown, UnknownOutcome: true}
}

func TestMutationUnknownOutcomeIsNotRetried(t *testing.T) {
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
			reader := newTestBTree(t, base, Config{HostId: 1})
			if test.delete {
				if err := reader.Insert(ctx, 7, 70); err != nil {
					t.Fatal(err)
				}
			}
			client := &unknownOutcomeClient{TxnClient: base, applyWrites: test.applyWrites}
			idx := newTestBTree(t, client, Config{HostId: 1})
			var err error
			if test.delete {
				err = idx.Delete(ctx, 7)
			} else {
				err = idx.Insert(ctx, 7, 70)
			}
			if !errors.Is(err, sinfoniaclient.ErrUnknownOutcome) {
				t.Errorf("mutation error = %v, want ErrUnknownOutcome", err)
			}
			if client.attempts != 1 {
				t.Errorf("commit attempts = %d, want 1", client.attempts)
			}
			wantFound := test.applyWrites
			if test.delete {
				wantFound = !test.applyWrites
			}
			value, found, err := reader.Search(ctx, 7)
			if err != nil || found != wantFound || (found && value != 70) {
				t.Fatalf("stored value = %d, found = %t, error = %v; want found = %t", value, found, err, wantFound)
			}
		})
	}
}
