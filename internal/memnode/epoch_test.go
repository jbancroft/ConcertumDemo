package memnode

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
)

func TestParticipantEpochSharedAndMonotonic(t *testing.T) {
	now := time.Unix(100*3600, 0)
	first, err := NewParticipant(1, 64, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	first.nowFn = func() time.Time { return now }
	if got := first.currentEpochLocked(); got != 100 {
		t.Fatalf("initial epoch=%d, want 100", got)
	}
	now = now.Add(3*time.Hour + 30*time.Minute)
	second, err := NewParticipant(2, 64, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.nowFn = func() time.Time { return now }
	for _, node := range []*Participant{first, second} {
		if got := node.currentEpochLocked(); got != 103 {
			t.Fatalf("shared epoch=%d, want 103", got)
		}
	}
	now = now.Add(-5 * time.Hour)
	for _, node := range []*Participant{first, second} {
		if got := node.currentEpochLocked(); got != 103 {
			t.Fatalf("clock rollback reduced epoch to %d", got)
		}
	}
}

func TestEpochFloorProtectsDiscardedHistory(t *testing.T) {
	for _, kind := range []string{"forced-abort", "cached reply"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := NewParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			now := time.Unix(100*3600, 0)
			node.nowFn = func() time.Time { return now }
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
			req := sinfonia.ExecPrepareRequest{TID: tid, CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("X")}}}
			gc := node.RunForcedAbortGC
			hasRecord := func() bool { return node.forcedAborts.IsForcedAbort(tid) }
			if kind == "forced-abort" {
				if _, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid}); err != nil {
					t.Fatal(err)
				}
			} else {
				if resp, err := node.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteBadCmp {
					t.Fatalf("prepare=%+v, err=%v", resp, err)
				}
				gc = func() error { return node.Recover(ctx) }
				hasRecord = func() bool { _, ok := node.prepareOutcomes[tid]; return ok }
			}
			now = now.Add(3 * time.Hour)
			// Prevent publishing the floor. History must remain until it is durable.
			floorPath := filepath.Join(dir, "epoch.floor")
			if err := os.Remove(floorPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(floorPath, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := gc(); err == nil {
				t.Fatal("GC succeeded without persisting the epoch floor")
			}
			if !hasRecord() {
				t.Fatal("failed GC discarded transaction history")
			}
			if err := os.Remove(floorPath); err != nil {
				t.Fatal(err)
			}
			if err := gc(); err != nil {
				t.Fatal(err)
			}
			if hasRecord() {
				t.Fatal("GC retained expired history")
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			now = now.Add(-3 * time.Hour)
			restarted.nowFn = func() time.Time { return now }
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if resp, err := restarted.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch || resp.CurrentEpoch != 103 {
				t.Fatalf("retry after restart=%+v, err=%v", resp, err)
			}
			if resp, err := restarted.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid}); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch || resp.CurrentEpoch != 103 {
				t.Fatalf("1PC after restart=%+v, err=%v", resp, err)
			}
		})
	}
}

func TestParticipantRejectsCorruptEpochFloor(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "epoch.floor"), []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if node, err := NewParticipant(1, 64, dir); err == nil {
		_ = node.Close()
		t.Fatal("started without a valid epoch floor")
	}
}

func TestForcedAbortGCRetainsRecentVoteAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	now := time.Unix(100*3600, 0)
	node.nowFn = func() time.Time { return now }
	old := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	if resp, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: old}); err != nil || resp.Vote != sinfonia.VoteBadForced {
		t.Fatalf("old forced abort = %+v, %v", resp, err)
	}
	now = now.Add(3 * time.Hour)
	recent := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 103}
	if resp, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: recent}); err != nil || resp.Vote != sinfonia.VoteBadForced {
		t.Fatalf("recent forced abort = %+v, %v", resp, err)
	}
	if err := node.RunForcedAbortGC(); err != nil {
		t.Fatal(err)
	}
	if node.forcedAborts.IsForcedAbort(old) {
		t.Fatal("GC did not remove the expired record")
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.nowFn = func() time.Time { return now }
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := restarted.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: recent, Participants: []uint32{1, 2}, HasWrites: true,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("late")}},
	})
	if err != nil || resp.Vote != sinfonia.VoteBadForced {
		t.Fatalf("late prepare after GC and restart = %+v, %v", resp, err)
	}
}

func TestBackgroundTransactionRetention(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	now := time.Unix(100*3600, 0)
	node.nowFn = func() time.Time { return now }
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	old := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	read := sinfonia.ExecCommit1PCRequest{TID: old, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}}
	if resp, err := node.ExecCommit1PC(ctx, read); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("read = %+v, %v", resp, err)
	}
	finished := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 100}
	pending := sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: 100}
	for _, tid := range []sinfonia.TransactionID{finished, pending} {
		req := sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 1, Len: 1}}}
		if resp, err := node.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare = %+v, %v", resp, err)
		}
	}
	if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: finished, ActionCommit: false}); err != nil || !resp.Ack {
		t.Fatalf("abort = %+v, %v", resp, err)
	}
	now = now.Add(time.Hour)
	recent := read
	recent.TID = sinfonia.TransactionID{CoordinatorID: 1, Counter: 4, Epoch: 101}
	if resp, err := node.ExecCommit1PC(ctx, recent); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("recent read = %+v, %v", resp, err)
	}
	now = now.Add(time.Hour)

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); node.RunRecovery(workerCtx, nil) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		node.mu.Lock()
		_, oldRead := node.onePCOutcomes[old]
		_, oldPrepare := node.prepareOutcomes[finished]
		_, oldCommit := node.commitOutcomes[finished]
		_, oldReadCommit := node.commitOutcomes[old]
		node.mu.Unlock()
		if !oldRead && !oldPrepare && !oldCommit && !oldReadCommit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background cleanup retained expired transaction outcomes")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	for _, tid := range []sinfonia.TransactionID{pending, recent.TID} {
		vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
		if err != nil || vote.Vote != sinfonia.VoteOk {
			t.Fatalf("retained reply for %v = %+v, %v", tid, vote, err)
		}
	}
	if _, ok := node.decided.Get(old); ok {
		t.Fatal("expired read-only decision retained")
	}

	if resp, err := node.ExecCommit1PC(ctx, read); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch {
		t.Fatalf("expired retry = %+v, %v", resp, err)
	}
	if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: old, ActionCommit: true}); err != nil || resp.Ack {
		t.Fatalf("expired decision = %+v, %v", resp, err)
	}
	if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: old}); err != nil || !resp.Ack {
		t.Fatalf("expired abort delivery = %+v, %v", resp, err)
	}

	if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: pending, ActionCommit: true}); err != nil || !resp.Ack {
		t.Fatalf("finish expired prepare = %+v, %v", resp, err)
	}
	node.mu.Lock()
	err = node.gcOutcomesLocked()
	node.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.prepareOutcomes[pending]; ok {
		t.Fatal("completed old transaction retained a cached prepare reply")
	}

}

func TestTransactionRetentionFailureAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	now := time.Unix(100*3600, 0)
	node.nowFn = func() time.Time { return now }
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	read := sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 2, Len: 1}}}
	if resp, err := node.ExecCommit1PC(ctx, read); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("read = %+v, %v", resp, err)
	}

	aborted := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 100}
	prepare := sinfonia.ExecPrepareRequest{TID: aborted, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("X")}}}
	if resp, err := node.ExecAndPrepare(ctx, prepare); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare = %+v, %v", resp, err)
	}
	if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: aborted}); err != nil || !resp.Ack {
		t.Fatalf("abort = %+v, %v", resp, err)
	}
	now = now.Add(2 * time.Hour)
	path := filepath.Join(dir, "epoch.floor")
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	err = node.gcOutcomesLocked()
	node.mu.Unlock()
	if err == nil {
		t.Fatal("cleanup succeeded with blocked storage")
	}
	if _, ok := node.onePCOutcomes[tid]; !ok {
		t.Fatal("failed cleanup discarded the cached reply")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	err = node.gcOutcomesLocked()
	node.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if committed, known := node.decided.Get(aborted); !known || committed {
		t.Fatal("cleanup lost the abort decision for retained redo")
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	now = now.Add(-2 * time.Hour)
	restarted.nowFn = func() time.Time { return now }
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if resp, err := restarted.ExecCommit1PC(ctx, read); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch {
		t.Fatalf("forgotten retry after clock rollback = %+v, %v", resp, err)
	}
	if got, err := restarted.ReadBytesForTest(1, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("recovery applied an aborted write: %q, %v", got, err)
	}
}

func TestStaleRequestPersistsEpochFloor(t *testing.T) {
	for _, kind := range []string{"prepare", "one-pc"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
			execute := func(node *Participant) (sinfonia.Vote, error) {
				if kind == "prepare" {
					resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid})
					return resp.Vote, err
				}
				resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid})
				return resp.Vote, err
			}
			now := time.Unix(100*3600, 0)
			for attempt := 0; attempt < 2; attempt++ {
				node, err := NewParticipant(1, 64, dir)
				if err != nil {
					t.Fatal(err)
				}
				defer node.Close()
				node.nowFn = func() time.Time { return now }
				if err := node.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				if attempt == 0 {
					now = now.Add(2 * time.Hour)
				}
				if vote, err := execute(node); err != nil || vote != sinfonia.VoteBadStaleEpoch {
					t.Fatalf("attempt %d = %v, %v", attempt, vote, err)
				}
				if err := node.Close(); err != nil {
					t.Fatal(err)
				}
				now = time.Unix(100*3600, 0)
			}
		})
	}
}

func TestRecoveryOfOldTransaction(t *testing.T) {
	for _, state := range []string{"prepared", "never voted", "retained commit"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(100*3600, 0)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(now)}
			a, err := NewParticipant(1, 16, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			b, err := NewParticipant(2, 16, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			defer b.Close()
			for _, node := range []*Participant{a, b} {
				node.nowFn = func() time.Time { return now }
				if err := node.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				if node == a && state == "never voted" {
					continue
				}
				resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{Data: []byte("X")}}})
				if err != nil || resp.Vote != sinfonia.VoteOk {
					t.Fatalf("prepare: %+v, %v", resp, err)
				}
			}
			if state == "retained commit" {
				if resp, err := a.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
					t.Fatalf("partial commit: %+v, %v", resp, err)
				}
			}
			now = now.Add(2 * time.Hour)
			// Run the same expiration used by background recovery. An unresolved
			// prepare stays pinned, and a completed decision remains in its log.
			for _, node := range []*Participant{a, b} {
				node.mu.Lock()
				err := node.gcOutcomesLocked()
				node.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			result := recovery.Resolve(ctx, tid, []uint32{1, 2}, map[uint32]recovery.Participant{1: a, 2: b})
			known := state != "never voted"
			if result.Known != known || result.Committed != known || (known && result.Err != nil) || (!known && !errors.Is(result.Err, ErrRecoveryPending)) {
				t.Fatalf("recovery after expiration: %+v", result)
			}
			if !known {
				// Startup recovery uses this other entry point to the same evidence.
				if _, err := a.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: tid}); !errors.Is(err, ErrRecoveryPending) {
					t.Fatalf("forgotten evidence became a vote: %v", err)
				}
			}
			pending, err := b.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
			wantPending := 1
			if known {
				wantPending = 0
			}
			if err != nil || len(pending.Entries) != wantPending {
				t.Fatalf("pending transactions: %+v, %v; want %d", pending, err, wantPending)
			}
			for _, node := range []*Participant{a, b} {
				want := "\x00"
				if known {
					want = "X"
				}
				if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != want {
					t.Fatalf("recovered data = %q, %v; want %q", got, err, want)
				}
			}
		})
	}
}
