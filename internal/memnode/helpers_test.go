package memnode_test

import (
	"context"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
)

func newTestParticipant(t testing.TB, id uint32, size int) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewParticipant(id, size, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if err := node.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return node
}

func assertParticipantReady(t *testing.T, node *memnode.Participant, want bool) {
	t.Helper()
	if got := node.ReadyForTest(); got != want {
		t.Fatalf("participant readiness = %t, want %t", got, want)
	}
}
