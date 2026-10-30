package memnode

import (
	"context"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestRateLimitRejectsGracefully(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{PrepareRate: 1, Burst: 1})
	_, err1 := n.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: sinfonia.TransactionID{Epoch: sinfonia.EpochAt(time.Now())}})
	_, err2 := n.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: sinfonia.TransactionID{Epoch: sinfonia.EpochAt(time.Now()), Counter: 2}})
	if err1 != nil && err1 == ErrResourceExhausted {
		t.Fatalf("first request should pass")
	}
	if err2 == nil {
		t.Fatalf("second request should be limited")
	}
}
