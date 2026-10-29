package client

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestCoordinatorLogsReadOnlyProjectionWhenTransactionHasWrites(t *testing.T) {
	root := t.TempDir()
	n1, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "n1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n1.Close() })
	n2, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "n2"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n2.Close() })

	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	ctx := context.Background()
	result := coord.ExecuteAndCommit(
		ctx,
		"",
		nil,
		[]sinfonia.ReadItem{{MemID: 2, Addr: 0, Len: 1}},
		[]sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	)
	if !result.Committed {
		t.Fatalf("transaction should commit: %+v", result)
	}
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}

	if got := n1.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("write participant redo entries=%d want=1", got)
	}
	if got := n2.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("read-only projection redo entries=%d want=1", got)
	}
}
