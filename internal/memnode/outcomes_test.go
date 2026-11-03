package memnode_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

// Duplicate requests preserve the original response without reapplying writes.

func TestExecCommit1PCDuplicateTIDReturnsOriginalOutcomeWithoutReapply(t *testing.T) {
	n := newTestParticipant(t, 1, 8)
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(context.Background(), req)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("first 1pc resp=%+v err=%v", resp, err)
	}
	retry := req
	retry.RequestID = "retry"
	retry.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecCommit1PC(context.Background(), retry)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("retry 1pc should return original outcome, resp=%+v err=%v", resp, err)
	}
	got, _ := n.ReadBytesForTest(0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate 1pc reapplied write: got %q", got)
	}
}

func TestPrepareAndCommitDuplicateTIDReturnOriginalOutcomeAfterResponseLoss(t *testing.T) {
	n := newTestParticipant(t, 1, 8)
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	prep := sinfonia.ExecPrepareRequest{TID: txId, RequestID: "prepare-1", Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	prep.RequestID = "prepare-retry"
	prep.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "prepare-retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare retry resp=%+v err=%v", resp, err)
	}
	commit := sinfonia.CommitRequest{TID: txId, RequestID: "commit-1", ActionCommit: true}
	cresp, err := n.Commit(context.Background(), commit)
	if err != nil || !cresp.Ack {
		t.Fatalf("commit resp=%+v err=%v", cresp, err)
	}
	commit.RequestID = "commit-retry"
	commit.ActionCommit = false
	cresp, err = n.Commit(context.Background(), commit)
	if err != nil || !cresp.Ack || cresp.RequestID != "commit-retry" {
		t.Fatalf("commit retry resp=%+v err=%v", cresp, err)
	}
	got, _ := n.ReadBytesForTest(0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate commit changed outcome: got %q", got)
	}
}

func TestDurableResponsesAreNotLoggedOrRestored(t *testing.T) {
	for _, onePC := range []bool{false, true} {
		name := "prepare"
		if onePC {
			name = "one-phase"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node := newRecoveredFailureTestNode(t, 1, dir)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			reads := []sinfonia.ReadItem{{Addr: 0, Len: 1}}
			writes := []sinfonia.WriteItem{{Addr: 8, Data: []byte("A")}}
			if onePC {
				resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: reads, WriteItems: writes})
				if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
					t.Fatalf("execute = %+v, %v", resp, err)
				}
			} else {
				resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, ReadItems: reads, WriteItems: writes})
				if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
					t.Fatalf("prepare = %+v, %v", resp, err)
				}
				if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
					t.Fatalf("commit = %+v, %v", resp, err)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, "replay.log")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("participant created a replay log: %v", err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			// The obsolete path must not be opened, even if it is unreadable as a log.
			if err := os.Mkdir(filepath.Join(dir, "replay.log"), 0o700); err != nil {
				t.Fatal(err)
			}
			node = newRecoveredFailureTestNode(t, 1, dir)
			if err := node.ApplyWriteForTest(0, []byte("B")); err != nil {
				t.Fatal(err)
			}
			if err := node.ApplyWriteForTest(8, []byte("Z")); err != nil {
				t.Fatal(err)
			}
			if onePC {
				resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: reads, WriteItems: writes})
				if !errors.Is(err, memnode.ErrResponseUnavailable) || resp.Vote != sinfonia.VoteBadInternal || len(resp.ReadData) != 0 {
					t.Fatalf("duplicate after restart = %+v, %v", resp, err)
				}
			} else {
				resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, ReadItems: reads, WriteItems: writes})
				if !errors.Is(err, memnode.ErrResponseUnavailable) || resp.Vote != sinfonia.VoteBadInternal || len(resp.ReadData) != 0 {
					t.Fatalf("duplicate after restart = %+v, %v", resp, err)
				}
			}
			vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
			if err != nil || !vote.KnownDecision || !vote.DecisionCommit {
				t.Fatalf("node recovery must preserve the logged commit: %+v, %v", vote, err)
			}
			if got, err := node.ReadBytesForTest(8, 1); err != nil || string(got) != "Z" {
				t.Fatalf("duplicate reapplied a write: %q, %v", got, err)
			}
		})
	}
}

func TestPrepareCrashAfterRedoBeforeResponseDoesNotReexecuteKnownTID(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	recoverTestParticipants(t, n)
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after redo")
	n.SetResponseBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
		if phase == "prepare-before-response" {
			return crash
		}
		return nil
	})
	txId := sinfonia.TransactionID{CoordinatorID: 75, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecPrepareRequest{
		TID:          txId,
		RequestID:    "first",
		Participants: []uint32{1},
		ReadItems:    []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	resp, err := n.ExecAndPrepare(ctx, req)
	if !errors.Is(err, crash) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("boundary response = %+v, err = %v", resp, err)
	}
	if got := n.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("redo entries before restart = %d, want 1", got)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(8, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	req.WriteItems[0].Data = []byte("B")
	resp, err = restarted.ExecAndPrepare(ctx, req)
	if !errors.Is(err, memnode.ErrResponseUnavailable) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	if got := restarted.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("retry appended redo: entries = %d, want 1", got)
	}
	got, err := restarted.ReadBytesForTest(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("retry reapplied write: got %q, want Z", got)
	}
}

func TestOnePCCrashAfterDecisionBeforeResponseDoesNotReexecuteKnownTID(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	now := time.Unix(100*3600, 0)
	n, err := memnode.NewParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	n.SetClockForTest(func() time.Time { return now })
	recoverTestParticipants(t, n)
	crash := errors.New("crash after decision")
	n.SetResponseBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
		if phase == "one-pc-before-response" {
			return crash
		}
		return nil
	})
	txId := sinfonia.TransactionID{CoordinatorID: 76, Counter: 1, Epoch: sinfonia.EpochAt(now)}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(ctx, req)
	if !errors.Is(err, crash) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("boundary response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.SetClockForTest(func() time.Time { return now })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(8, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	req.WriteItems[0].Data = []byte("B")
	resp, err = restarted.ExecCommit1PC(ctx, req)
	if !errors.Is(err, memnode.ErrResponseUnavailable) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	if got := restarted.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("retry appended redo: entries = %d, want 1", got)
	}
	got, err := restarted.ReadBytesForTest(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("retry reapplied write: got %q, want Z", got)
	}
	if err := restarted.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if got := restarted.RedoEntryCountForTest(); got != 0 {
		t.Fatalf("redo entries after cleanup = %d, want 0", got)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("decision survived redo cleanup")
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err = memnode.NewParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetClockForTest(func() time.Time { return now })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("collected decision reappeared after restart")
	}
	if got, err := restarted.ReadBytesForTest(8, 1); err != nil || string(got) != "Z" {
		t.Fatalf("restart after cleanup changed data: %q, %v", got, err)
	}
}

func TestOnePCRetryWaitsForPendingRecoveryWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}
	injected := errors.New("interrupted before reply")
	node.SetResponseBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return injected })
	if _, err := node.ExecCommit1PC(ctx, req); !errors.Is(err, injected) {
		t.Fatalf("initial call: %v", err)
	}
	node.SetResponseBoundaryHookForTest(nil)
	node.SetWriteApplyFaultForTest(func(uint64, []byte) error { return injected })
	if _, err := node.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true}); !errors.Is(err, injected) {
		t.Fatalf("recovery: %v", err)
	}
	node.SetWriteApplyFaultForTest(nil)
	if _, err := node.ExecCommit1PC(ctx, req); !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Fatalf("retry before recovery = %v, want ErrRecoveryPending", err)
	}
	if node.ActiveLockCountForTest() == 0 {
		t.Fatal("failed recovery released pending write locks")
	}
	recoverTestParticipants(t, node)
	if resp, err := node.ExecCommit1PC(ctx, req); err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("retry = %+v, %v", resp, err)
	}
	if node.ActiveLockCountForTest() != 0 {
		t.Fatal("successful retry retained locks")
	}
	image, err := os.ReadFile(filepath.Join(dir, "image.bin"))
	if err != nil || string(image[:1]) != "A" {
		t.Fatalf("retry left disk unapplied: %q, %v", image, err)
	}
}
