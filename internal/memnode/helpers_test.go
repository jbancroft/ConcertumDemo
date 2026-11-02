package memnode_test

import (
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"testing"
)

func assertParticipantReady(t *testing.T, node *memnode.Participant, want bool) {
	t.Helper()
	if got := node.ReadyForTest(); got != want {
		t.Fatalf("participant readiness = %t, want %t", got, want)
	}
}
