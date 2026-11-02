package memnode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestParticipantEpochSharedAndMonotonic(t *testing.T) {
	now := time.Unix(100*3600, 0)
	first := NewParticipant(64)
	defer first.Close()
	first.nowFn = func() time.Time { return now }
	if got := first.currentEpochLocked(); got != 100 {
		t.Fatalf("initial epoch=%d, want 100", got)
	}
	now = now.Add(3*time.Hour + 30*time.Minute)
	second := NewParticipant(64)
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
			node, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			now := time.Unix(100*3600, 0)
			node.nowFn = func() time.Time { return now }
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
			restarted, err := NewDurableParticipant(1, 64, dir)
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
	if node, err := NewDurableParticipant(1, 64, dir); err == nil {
		_ = node.Close()
		t.Fatal("started without a valid epoch floor")
	}
}

func TestForcedAbortGCRetainsRecentVoteAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := NewDurableParticipant(1, 64, dir)
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
	restarted, err := NewDurableParticipant(1, 64, dir)
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
	for _, mode := range []string{"ram", "durable"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node := NewParticipantWithId(1, 64)
			if mode != "ram" {
				var err error
				node, err = NewDurableParticipant(1, 64, dir)
				if err != nil {
					t.Fatal(err)
				}
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
				if err != nil || vote.Vote != sinfonia.VoteOk || len(vote.ReadData) != 1 {
					t.Fatalf("retained reply for %v = %+v, %v", tid, vote, err)
				}
			}
			if node.decided != nil {
				if _, ok := node.decided.Get(old); ok {
					t.Fatal("expired read-only decision retained")
				}
			}
			vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: old})
			if err != nil || !vote.Expired || vote.KnownDecision {
				t.Fatalf("forgotten outcome = %+v, %v", vote, err)
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
			vote, err = node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: old})
			if err != nil || !vote.Expired {
				t.Fatalf("late requests recreated evidence: %+v, %v", vote, err)
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
			vote, err = node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: pending})
			if err != nil || !vote.Expired {
				t.Fatalf("completed old transaction retained: %+v, %v", vote, err)
			}

		})
	}
}

func TestTransactionRetentionFailureAndRestart(t *testing.T) {
	for _, blocked := range []string{"epoch.floor", "decided.log"} {
		t.Run(blocked, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := NewDurableParticipant(1, 64, dir)
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
			write := sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 2, Data: []byte("A")}}}
			if resp, err := node.ExecCommit1PC(ctx, write); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("write = %+v, %v", resp, err)
			}
			// Leave a decision eligible for expiration after its redo is removed.
			if err := node.RunRedoLogGC(ctx); err != nil {
				t.Fatal(err)
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
			path := filepath.Join(dir, blocked)
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
			restarted, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			now = now.Add(-2 * time.Hour)
			restarted.nowFn = func() time.Time { return now }
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if resp, err := restarted.ExecCommit1PC(ctx, write); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch {
				t.Fatalf("forgotten retry after clock rollback = %+v, %v", resp, err)
			}
			if got, err := restarted.ReadBytesForTest(1, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("recovery applied an aborted write: %q, %v", got, err)
			}
		})
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
				node, err := NewDurableParticipant(1, 64, dir)
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
