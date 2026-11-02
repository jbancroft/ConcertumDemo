package memnode_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Log and epoch cleanup preserve the evidence required for subsequent recovery.

func TestRedoLogGCCommitAfterAllApplied(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	b, _ := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 11, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("a")}}})
	_, _ = b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("b")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	_, _ = b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected redo gc to truncate committed entry")
	}
	if !a.HasDecidedForTest(txId) {
		t.Fatalf("expected recent decision retained after redo cleanup")
	}
	if a.HasAppliedTrackingForTest(txId) {
		t.Fatalf("expected applied tracking cleaned")
	}
}

func TestRedoLogGCNoPrematureTruncationWithoutRemoteConfirm(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 12, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 1 {
		t.Fatalf("expected redo entry to remain without remote applied confirmation")
	}
}

func TestRedoLogGCAbortRemovedImmediately(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 13, Epoch: sinfonia.EpochAt(time.Now())}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 2, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false})

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected abort entry gc immediately")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	vote, err := restarted.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: txId})
	if err != nil || !vote.KnownDecision || vote.DecisionCommit {
		t.Fatalf("aborted outcome after redo cleanup and restart = %+v, %v", vote, err)
	}
}

func TestForcedAbortEpochGC(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	n, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "n"))
	t.Cleanup(func() { _ = n.Close() })
	base := time.Unix(3600, 0)
	n.SetClockForTest(func() time.Time { return base })
	old := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(base)}
	recent := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 5}
	_, _ = n.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: old})
	_, _ = n.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: recent})
	base = base.Add(6 * time.Hour)

	if err := n.RunForcedAbortGC(); err != nil {
		t.Fatal(err)
	}
	if n.HasForcedAbortForTest(old) {
		t.Fatalf("expected old forced-abort removed")
	}
	if !n.HasForcedAbortForTest(recent) {
		t.Fatalf("expected recent forced-abort preserved")
	}
}

func TestRecoveryAfterPartialGC(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	bDir := filepath.Join(root, "b")
	a, _ := memnode.NewDurableParticipant(1, 64, aDir)
	b, _ := memnode.NewDurableParticipant(2, 64, bDir)
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 5, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 7, Data: []byte("x")}}})
	_, _ = b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 7, Data: []byte("y")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	_, _ = b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)
	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	// Reject a smaller image without losing the committed byte at address 7.
	if smaller, err := memnode.NewDurableParticipant(1, 4, aDir); err == nil {
		_ = smaller.Close()
		t.Error("expected reopening with a smaller size to fail")
	}
	a2, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.ReadBytesForTest(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "x" {
		t.Fatalf("expected recovered value x, got %q", got)
	}
}

func TestRecoveryAfterGCWithProcessedCheckpoint(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "partial"}[partial], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			open := func() *memnode.Participant {
				n, err := memnode.NewDurableParticipant(1, 16, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = n.Close() })
				return n
			}
			node := open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			for i, value := range []byte("AB") {
				parts := []uint32{1}
				if partial && i == 1 {
					// The second entry must remain until the other participant
					// confirms application, even though its decision is known.
					parts = []uint32{1, 2}
				}
				tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: sinfonia.EpochAt(time.Now())}
				prepared, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
					TID: tid, Participants: parts,
					WriteItems: []sinfonia.WriteItem{{Addr: uint64(i), Data: []byte{value}}},
				})
				if err != nil || prepared.Vote != sinfonia.VoteOk {
					t.Fatalf("prepare: %+v, %v", prepared, err)
				}
				if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
					t.Fatalf("commit: %+v, %v", resp, err)
				}
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node = open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if ptr, err := storage.LoadProcessedPointer(dir); err != nil || ptr != 2 {
				t.Fatalf("checkpoint before GC = %d, %v; want 2", ptr, err)
			}
			if err := node.RunRedoLogGC(ctx); err != nil {
				t.Fatal(err)
			}
			wantRemaining := 0
			if partial {
				wantRemaining = 1
			}
			if got := node.RedoEntryCountForTest(); got != wantRemaining {
				t.Fatalf("remaining redo entries = %d, want %d", got, wantRemaining)
			}

			// Stop after the new commit decision is durable, before its write.
			interrupted := errors.New("interrupted before applying committed write")
			node.SetResponseBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
				if phase == "one-pc-before-response" {
					return interrupted
				}
				return nil
			})
			_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
				TID:        sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: sinfonia.EpochAt(time.Now())},
				WriteItems: []sinfonia.WriteItem{{Addr: 2, Data: []byte("C")}},
			})
			if !errors.Is(err, interrupted) {
				t.Fatalf("commit interruption: %v", err)
			}
			if got, err := node.ReadBytesForTest(2, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("write applied before restart: %q, %v", got, err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node = open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if got, err := node.ReadBytesForTest(0, 3); err != nil || string(got) != "ABC" {
				t.Fatalf("recovered data = %q, %v; want ABC", got, err)
			}
		})
	}
}

func TestRedoLogGCRejectsConcurrentStartupRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("initial commit: %+v, %v", resp, err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	entered, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	node.SetRecoverBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
		if phase == "replay-before" {
			close(entered)
			select {
			case <-proceed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	done := make(chan struct{})
	var recoverErr error
	go func() { defer close(done); recoverErr = node.Recover(ctx) }()
	defer func() { release(); <-done }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := node.RunRedoLogGC(ctx); !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Errorf("GC during recovery = %v, want ErrRecoveryPending", err)
	}
	if got := node.RedoEntryCountForTest(); got != 1 {
		t.Errorf("GC changed recovery's log: %d entries", got)
	}
	release()
	<-done
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := node.RunRedoLogGC(ctx); err != nil {
		t.Fatalf("GC after recovery: %v", err)
	}
}

func TestRedoLogGCCheckpointFailurePreservesLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("initial commit: %+v, %v", resp, err)
	}
	// Block checkpoint publication without preventing writes to the redo log.
	if err := os.Mkdir(filepath.Join(dir, "processed.ptr"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := node.RunRedoLogGC(ctx); err == nil {
		t.Error("GC succeeded despite checkpoint failure")
	}
	if node.RedoEntryCountForTest() != 1 || !node.HasDecidedForTest(tid) {
		t.Error("failed GC discarded recovery evidence")
	}
	assertParticipantReady(t, node, false)
	if err := os.Remove(filepath.Join(dir, "processed.ptr")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
		t.Fatalf("data after failed GC and restart = %q, %v", got, err)
	}
}
