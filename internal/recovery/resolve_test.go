package recovery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
)

type unavailable struct{ recovery.Participant }

func (unavailable) RecoveryForceAbort(ctx context.Context, _ sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	<-ctx.Done()
	return sinfonia.RecoverForceAbortResponse{}, ctx.Err()
}

func newTestParticipant(t testing.TB, id uint32, size int) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewParticipant(id, size, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := node.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	return node
}

func TestKnownDecisionStillReachesPreparedNodeWithAnotherPeerUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	decided, prepared := newTestParticipant(t, 2, 16), newTestParticipant(t, 3, 16)
	for _, node := range []*memnode.Participant{decided, prepared} {
		resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2, 3}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}})
		if err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare: %+v, %v", resp, err)
		}
	}
	if _, err := decided.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}
	result := recovery.Resolve(ctx, tid, []uint32{1, 2, 3}, map[uint32]recovery.Participant{1: unavailable{}, 2: decided, 3: prepared})
	if !result.Known || !result.Committed {
		t.Fatalf("lost known decision: %+v", result)
	}
	pending, err := prepared.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 0 {
		t.Fatalf("unreachable peer prevented decision delivery: %+v, %v", pending, err)
	}
	response, err := prepared.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 2, Counter: 1, Epoch: tid.Epoch}, ReadItems: []sinfonia.ReadItem{{Len: 1}}})
	if err != nil || response.Vote != sinfonia.VoteOk || string(response.ReadData[0]) != "A" {
		t.Fatalf("prepared write not visible: %+v, %v", response, err)
	}
}

func TestDiscoverRejectsConflictingDecisions(t *testing.T) {
	ctx := t.Context()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	committed, aborted := newTestParticipant(t, 1, 16), newTestParticipant(t, 2, 16)
	for _, node := range []*memnode.Participant{committed, aborted} {
		if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
			TID: tid, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}},
		}); err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare = %+v, %v", resp, err)
		}
		// Inject contradictory durable decisions to verify fail-closed discovery.
		if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: node == committed}); err != nil || !resp.Ack {
			t.Fatalf("decision = %+v, %v", resp, err)
		}
	}
	result := recovery.Discover(ctx, tid, []uint32{1, 2}, map[uint32]recovery.Participant{1: committed, 2: aborted})
	if result.Known || result.Err == nil {
		t.Fatalf("conflicting evidence produced a decision: %+v", result)
	}
}

func TestCanceledDiscoveryDoesNotForceAnAbort(t *testing.T) {
	node := newTestParticipant(t, 1, 16)
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := recovery.Discover(ctx, tid, []uint32{1}, map[uint32]recovery.Participant{1: node})
	if result.Known || !errors.Is(result.Err, context.Canceled) || node.HasForcedAbortForTest(tid) {
		t.Fatalf("canceled discovery changed transaction evidence: %+v", result)
	}
}
