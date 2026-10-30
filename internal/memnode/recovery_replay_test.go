package memnode_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestReplaySaveFailureRetainsPendingResponse(t *testing.T) {
	for _, tc := range []struct {
		name         string
		onePC        bool
		failOriginal bool
	}{{"prepare-recovery", false, false}, {"one-phase-recovery", true, false}, {"one-phase-execution", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := memnode.NewDurableParticipant(1, 16, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			fault := errors.New("interrupted before saving response")
			if !tc.failOriginal {
				node.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return fault })
			}
			replayPath := filepath.Join(dir, "replay.log")
			blockReplay := func() {
				t.Helper()
				if err := os.Remove(replayPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(replayPath, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if tc.failOriginal {
				blockReplay()
			}
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			if tc.onePC {
				_, err = node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, RequestID: "original", ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}})
			} else {
				_, err = node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, RequestID: "original", Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}})
			}
			if err == nil || (!tc.failOriginal && !errors.Is(err, fault)) {
				t.Fatalf("expected interrupted response: %v", err)
			}
			node.SetOutcomeReplayBoundaryHookForTest(nil)
			if !tc.failOriginal {
				blockReplay()
			}
			decision := sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true}
			if response, err := node.RecoveryDecide(ctx, decision); err == nil || response.Ack {
				t.Fatalf("recovery acknowledged an unsaved response: %+v, %v", response, err)
			}
			pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
			if err != nil || len(pending.Entries) != 1 {
				t.Fatalf("failed save discarded pending state: %+v, %v", pending, err)
			}
			if err := os.Remove(replayPath); err != nil {
				t.Fatal(err)
			}
			probe := sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 2, Counter: 1, Epoch: tid.Epoch}, ReadItems: []sinfonia.ReadItem{{Len: 1}}}
			if response, err := node.ExecCommit1PC(ctx, probe); err != nil || response.Vote != sinfonia.VoteBadLock {
				t.Fatalf("failed save released locks: %+v, %v", response, err)
			}
			if response, err := node.RecoveryDecide(ctx, decision); err != nil || !response.Ack {
				t.Fatalf("retry could not save the original response: %+v, %v", response, err)
			}
			probe.TID.Counter++
			if response, err := node.ExecCommit1PC(ctx, probe); err != nil || response.Vote != sinfonia.VoteOk || string(response.ReadData[0]) != "A" {
				t.Fatalf("recovery did not finish writes and release locks: %+v, %v", response, err)
			}
			// A later value must never replace the original read or be overwritten.
			if err := node.ApplyWriteForTest(0, []byte("B")); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
				if err != nil || !vote.KnownDecision || !vote.DecisionCommit || string(vote.ReadData[0]) != "\x00" {
					t.Fatalf("saved recovery response changed: %+v, %v", vote, err)
				}
				if response, err := node.RecoveryDecide(ctx, decision); err != nil || !response.Ack {
					t.Fatalf("repeated recovery failed: %+v, %v", response, err)
				}
			}
			if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "B" {
				t.Fatalf("repeated recovery overwrote a newer value: %q, %v", got, err)
			}
		})
	}
}

type firstReplayGate struct {
	*memnode.Participant
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (r *firstReplayGate) ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	if r.calls.Add(1) == 1 {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return sinfonia.ReplicateReplayRecordResponse{}, ctx.Err()
		}
	}
	return r.Participant.ReplicateReplayRecord(ctx, req)
}

func TestOnePCReplyRetryDoesNotReleaseNewerTransactionLocks(t *testing.T) {
	t.Run("execution", func(t *testing.T) { testConcurrentReplayCompletion(t, false) })
	t.Run("recovery", func(t *testing.T) { testConcurrentReplayCompletion(t, true) })
}

func testConcurrentReplayCompletion(t *testing.T, recoveryFirst bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	primary, err := memnode.NewDurableParticipant(1, 16, filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replica, err := memnode.NewDurableParticipant(2, 16, filepath.Join(dir, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.ConfigureAsReplica(1)
	gate := &firstReplayGate{Participant: replica, entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(gate.release) })
	primary.ConfigureAsPrimary(2, gate)
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}
	if recoveryFirst {
		fault := errors.New("interrupted before saving response")
		primary.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return fault })
		if _, err := primary.ExecCommit1PC(ctx, req); !errors.Is(err, fault) {
			t.Fatalf("expected interrupted request: %v", err)
		}
		primary.SetOutcomeReplayBoundaryHookForTest(nil)
	}
	done := make(chan struct{})
	var originalErr error
	go func() {
		defer close(done)
		if recoveryFirst {
			_, originalErr = primary.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true})
		} else {
			_, originalErr = primary.ExecCommit1PC(ctx, req)
		}
	}()
	defer func() { release(); <-done }()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if response, err := primary.ExecCommit1PC(ctx, req); err != nil || response.Vote != sinfonia.VoteOk || string(response.ReadData[0]) != "\x00" {
		t.Fatalf("retry did not preserve original response: %+v, %v", response, err)
	}
	newTID := sinfonia.TransactionID{CoordinatorID: 2, Counter: 1, Epoch: tid.Epoch}
	if response, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: newTID, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{Data: []byte("B")}}}); err != nil || response.Vote != sinfonia.VoteOk {
		t.Fatalf("successful retry left old locks: %+v, %v", response, err)
	}
	release()
	<-done
	if originalErr != nil {
		t.Fatal(originalErr)
	}
	probe := sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 3, Counter: 1, Epoch: tid.Epoch}, ReadItems: []sinfonia.ReadItem{{Len: 1}}}
	if response, err := primary.ExecCommit1PC(ctx, probe); err != nil || response.Vote != sinfonia.VoteBadLock {
		t.Fatalf("old reply completion released newer locks: %+v, %v", response, err)
	}
	if response, err := primary.Commit(ctx, sinfonia.CommitRequest{TID: newTID, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("new transaction failed: %+v, %v", response, err)
	}
	replica.PromoteToPrimary(0, nil)
	if response, err := replica.ExecCommit1PC(ctx, req); err != nil || response.Vote != sinfonia.VoteOk || string(response.ReadData[0]) != "\x00" {
		t.Fatalf("replica lost the original response: %+v, %v", response, err)
	}
	if got, err := replica.ReadBytesForTest(0, 1); err != nil || string(got) != "B" {
		t.Fatalf("replica replay overwrote newer data: %q, %v", got, err)
	}
}

func TestRecoveryReplayReplicationHonorsDeadline(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	primary, err := memnode.NewDurableParticipant(1, 16, filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replica, err := memnode.NewDurableParticipant(2, 16, filepath.Join(dir, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.ConfigureAsReplica(1)
	gate := &firstReplayGate{Participant: replica, entered: make(chan struct{}), release: make(chan struct{})}
	primary.ConfigureAsPrimary(2, gate)
	fault := errors.New("interrupted before saving response")
	primary.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return fault })
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if _, err := primary.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); !errors.Is(err, fault) {
		t.Fatalf("expected interrupted request: %v", err)
	}
	primary.SetOutcomeReplayBoundaryHookForTest(nil)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	var response sinfonia.RecoverDecideResponse
	var recoveryErr error
	go func() {
		defer close(done)
		response, recoveryErr = primary.RecoveryDecide(short, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true})
	}()
	defer func() { close(gate.release); <-done }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replay replication ignored the recovery deadline")
	}
	if !errors.Is(recoveryErr, context.DeadlineExceeded) || response.Ack {
		t.Fatalf("expired replay replication was acknowledged: %+v, %v", response, recoveryErr)
	}
	pending, err := primary.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 1 {
		t.Fatalf("expired replay replication discarded pending response: %+v, %v", pending, err)
	}
	if response, err := primary.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("replay replication could not be retried: %+v, %v", response, err)
	}
}
