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
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Log and epoch cleanup preserve the evidence required for subsequent recovery.

func commitGCTransaction(t *testing.T, a, b *memnode.Participant, counter uint64) sinfonia.TransactionID {
	t.Helper()
	ctx := context.Background()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: counter, Epoch: sinfonia.EpochAt(time.Now())}
	for i, node := range []*memnode.Participant{a, b} {
		resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
			TID: tid, Participants: []uint32{1, 2}, HasWrites: true,
			WriteItems: []sinfonia.WriteItem{{MemID: uint32(i + 1), Addr: counter - 1, Data: []byte("X")}},
		})
		if err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare node %d: %+v, %v", i+1, resp, err)
		}
	}
	for _, node := range []*memnode.Participant{a, b} {
		if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
			t.Fatalf("commit: %+v, %v", resp, err)
		}
	}
	return tid
}

func TestRedoLogGCAfterPeerCollectedAndReceiverRestarted(t *testing.T) {
	ctx := context.Background()
	a := newRecoveredFailureTestNode(t, 1, t.TempDir())
	bDir := t.TempDir()
	b := newRecoveredFailureTestNode(t, 2, bDir)
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	tid := commitGCTransaction(t, a, b, 1)
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)
	if err := a.RunRedoLogGC(ctx); err != nil || a.RedoEntryCountForTest() != 0 {
		t.Fatalf("first node GC: %v", err)
	}
	// A later entry must also be collected once the older entry stops blocking.
	later := tid
	later.Counter++
	if resp, err := b.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		TID: later, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 1, Data: []byte("Y")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("later write: %+v, %v", resp, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = newRecoveredFailureTestNode(t, 2, bDir)
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := b.RunRedoLogGC(ctx); err != nil || b.RedoEntryCountForTest() != 0 {
		t.Fatalf("restarted node retained completed prefix: %d entries, %v", b.RedoEntryCountForTest(), err)
	}
	if got, err := b.ReadBytesForTest(0, 2); err != nil || string(got) != "XY" {
		t.Fatalf("GC changed recovered data: %q, %v", got, err)
	}
	if receipts, err := storage.LoadAppliedReceipts(bDir); err != nil || len(receipts) != 0 {
		t.Fatalf("collected receipt snapshot = %v, %v", receipts, err)
	}
}

type failingAppliedPeer struct {
	memnode.RecoveryPeer
	fail      atomic.Bool
	dropReply bool
}

func (p *failingAppliedPeer) NotifyApplied(ctx context.Context, req sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	if !p.fail.Load() {
		return p.RecoveryPeer.NotifyApplied(ctx, req)
	}
	if p.dropReply {
		if _, err := p.RecoveryPeer.NotifyApplied(ctx, req); err != nil {
			return sinfonia.NotifyAppliedResponse{}, err
		}
	}
	return sinfonia.NotifyAppliedResponse{}, errors.New("lost applied notification or acknowledgment")
}

func TestRedoLogGCRetainsUndeliveredNotifications(t *testing.T) {
	for _, dropReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "lost request", true: "lost acknowledgment"}[dropReply], func(t *testing.T) {
			ctx := context.Background()
			aDir := t.TempDir()
			a := newRecoveredFailureTestNode(t, 1, aDir)
			b := newRecoveredFailureTestNode(t, 2, t.TempDir())
			peer := &failingAppliedPeer{RecoveryPeer: b, dropReply: dropReply}
			peer.fail.Store(true)
			a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: peer})
			b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
			commitGCTransaction(t, a, b, 1)
			b.FlushAppliedNotifications(ctx)
			if err := a.RunRedoLogGC(ctx); err != nil || a.RedoEntryCountForTest() != 1 {
				t.Fatalf("GC discarded undelivered evidence: %d entries, %v", a.RedoEntryCountForTest(), err)
			}
			// Restart must reconstruct the outgoing queue from retained redo.
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			a = newRecoveredFailureTestNode(t, 1, aDir)
			a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: peer})
			b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
			peer.fail.Store(false)
			for _, node := range []*memnode.Participant{a, b} {
				if err := node.RunRedoLogGC(ctx); err != nil || node.RedoEntryCountForTest() != 0 {
					t.Fatalf("GC did not resume after delivery: %d entries, %v", node.RedoEntryCountForTest(), err)
				}
			}
		})
	}
}

func TestAppliedReceiptWriteFailureDoesNotAcknowledge(t *testing.T) {
	ctx := context.Background()
	a := newRecoveredFailureTestNode(t, 1, t.TempDir())
	bDir := t.TempDir()
	b := newRecoveredFailureTestNode(t, 2, bDir)
	// Delay A's outgoing delivery until the receipt destination is blocked.
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	tid := commitGCTransaction(t, a, b, 1)
	b.FlushAppliedNotifications(ctx)
	path := filepath.Join(bDir, "applied.gob")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	req := sinfonia.NotifyAppliedRequest{FromMemID: 1, TIDs: []sinfonia.TransactionID{tid}}
	// The duplicate must not acknowledge the receipt left only in memory.
	for attempt := 0; attempt < 2; attempt++ {
		if resp, err := b.NotifyApplied(ctx, req); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
			t.Fatalf("failed receipt acknowledged: %+v, %v", resp, err)
		}
	}
	assertParticipantReady(t, b, false)
	if err := b.Recover(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted instance recovered: %v", err)
	}
	if err := b.RunRedoLogGC(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted instance collected redo: %v", err)
	}
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a.RunRedoLogGC(ctx); err != nil || a.RedoEntryCountForTest() != 1 {
		t.Fatalf("sender discarded unacknowledged evidence: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	b = newRecoveredFailureTestNode(t, 2, bDir)
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	for _, node := range []*memnode.Participant{a, b} {
		if err := node.RunRedoLogGC(ctx); err != nil || node.RedoEntryCountForTest() != 0 {
			t.Fatalf("GC after repair: %d entries, %v", node.RedoEntryCountForTest(), err)
		}
	}
}

func TestAppliedReceiptCleanupResumesAfterRestart(t *testing.T) {
	ctx := context.Background()
	aDir := t.TempDir()
	a := newRecoveredFailureTestNode(t, 1, aDir)
	b := newRecoveredFailureTestNode(t, 2, t.TempDir())
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	removed := commitGCTransaction(t, a, b, 1)
	retained := commitGCTransaction(t, a, b, 2)
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// Emulate a crash after redo replacement, before receipt/decision cleanup.
	redo, err := storage.OpenRedoLog(aDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := redo.TruncateHead(1); err != nil {
		t.Fatal(err)
	}
	a = newRecoveredFailureTestNode(t, 1, aDir)
	receipts, err := storage.LoadAppliedReceipts(aDir)
	if err != nil || len(receipts) != 1 || !receipts[retained][2] || receipts[removed][2] {
		t.Fatalf("startup lost retained receipts or kept orphan receipts: %v, %v", receipts, err)
	}
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := a.RunRedoLogGC(ctx); err != nil || a.RedoEntryCountForTest() != 0 {
		t.Fatalf("remaining redo could not be collected: %v", err)
	}
}

func TestRedoLogGCCommitAfterAllApplied(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewParticipant(1, 64, filepath.Join(root, "a"))
	b, _ := memnode.NewParticipant(2, 64, filepath.Join(root, "b"))
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
	if a.HasDecidedForTest(txId) {
		t.Fatalf("expected decision removed with redo")
	}
	if a.HasAppliedTrackingForTest(txId) {
		t.Fatalf("expected applied tracking cleaned")
	}
}

func TestRedoLogGCNoPrematureTruncationWithoutRemoteConfirm(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewParticipant(1, 64, filepath.Join(root, "a"))
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
	if a.RedoEntryCountForTest() != 1 || !a.HasDecidedForTest(txId) {
		t.Fatalf("expected redo and decision to remain without remote applied confirmation")
	}
}

func TestRedoLogGCAbortRemovedImmediately(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewParticipant(1, 64, filepath.Join(root, "a"))
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
	restarted, err := memnode.NewParticipant(1, 64, filepath.Join(root, "a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.HasDecidedForTest(txId) || restarted.RedoEntryCountForTest() != 0 {
		t.Fatal("aborted redo or decision survived cleanup and restart")
	}
	if got, err := restarted.ReadBytesForTest(2, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("aborted write was applied: %q, %v", got, err)
	}
}

func TestRedoLogGCFailurePreservesRecoveryEvidence(t *testing.T) {
	for _, blocked := range []string{"redo.log", "decided.log"} {
		t.Run(blocked, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node := newRecoveredFailureTestNode(t, 1, dir)
			collected := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			retained := collected
			retained.Counter++
			if resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: collected, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("local commit: %+v, %v", resp, err)
			}
			// This entry must remain until participant 2 confirms application.
			if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: retained, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{Addr: 1, Data: []byte("B")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("prepare: %+v, %v", resp, err)
			}
			if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: retained, ActionCommit: true}); err != nil || !resp.Ack {
				t.Fatalf("commit: %+v, %v", resp, err)
			}
			path := filepath.Join(dir, blocked)
			if err := os.Rename(path, path+".saved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := node.RunRedoLogGC(ctx); err == nil {
				t.Fatal("GC succeeded with blocked storage")
			}
			assertParticipantReady(t, node, false)
			if !node.HasDecidedForTest(collected) || !node.HasDecidedForTest(retained) {
				t.Fatal("failed cleanup discarded a decision")
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".saved", path); err != nil {
				t.Fatal(err)
			}
			// Reopening after a decision-write failure starts with the new redo
			// and old decisions, as after a crash between the two replacements.
			restarted := newRecoveredFailureTestNode(t, 1, dir)
			wantRedo := 1
			if blocked == "redo.log" {
				wantRedo = 2
			}
			if got := restarted.RedoEntryCountForTest(); got != wantRedo {
				t.Fatalf("redo entries after restart = %d, want %d", got, wantRedo)
			}
			if restarted.HasDecidedForTest(collected) != (blocked == "redo.log") || !restarted.HasDecidedForTest(retained) {
				t.Fatal("recovery did not retain exactly the decisions with redo")
			}
			if got, err := restarted.ReadBytesForTest(0, 2); err != nil || string(got) != "AB" {
				t.Fatalf("recovery changed committed data: %q, %v", got, err)
			}
		})
	}
}

func TestForcedAbortEpochGC(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	n, _ := memnode.NewParticipant(1, 64, filepath.Join(root, "n"))
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
	a, _ := memnode.NewParticipant(1, 64, aDir)
	b, _ := memnode.NewParticipant(2, 64, bDir)
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
	if smaller, err := memnode.NewParticipant(1, 4, aDir); err == nil {
		_ = smaller.Close()
		t.Error("expected reopening with a smaller size to fail")
	}
	a2, err := memnode.NewParticipant(1, 64, aDir)
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
				n, err := memnode.NewParticipant(1, 16, dir)
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
	node, err := memnode.NewParticipant(1, 16, dir)
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
	node, err = memnode.NewParticipant(1, 16, dir)
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
	node, err := memnode.NewParticipant(1, 16, dir)
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
	node, err = memnode.NewParticipant(1, 16, dir)
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
