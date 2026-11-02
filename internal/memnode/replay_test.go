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
	n := memnode.NewParticipant(8)
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
	n := memnode.NewParticipant(8)
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

func TestDurableOnePCReplaysOriginalReadPayloadWithoutReapplyingWrite(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 71, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{
		TID:        txId,
		RequestID:  "first",
		ReadItems:  []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	resp, err := n.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(8, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	req.WriteItems[0].Data = []byte("B")
	resp, err = restarted.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "retry" || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	got, err := restarted.ReadBytesForTest(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("retry reapplied a write: got %q, want Z", got)
	}
}

func TestDurableSuccessfulPrepareReplaysOriginalReadPayloadAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 74, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecPrepareRequest{
		TID:          txId,
		RequestID:    "first",
		Participants: []uint32{1},
		ReadItems:    []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
	}
	resp, err := n.ExecAndPrepare(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := restarted.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: txId})
	if err != nil || status.KnownDecision || !status.VotedOK {
		t.Fatalf("restarted vote status = %+v, err = %v", status, err)
	}
	if err := restarted.ApplyWriteForTest(0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	resp, err = restarted.ExecAndPrepare(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "retry" || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
}

func TestDurableNegativePrepareOutcomeReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 72, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecPrepareRequest{
		TID:          txId,
		RequestID:    "first",
		Participants: []uint32{1},
		CmpItems:     []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}},
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 4, Data: []byte("A")}},
	}
	resp, err := n.ExecAndPrepare(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteBadCmp {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(0, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	resp, err = restarted.ExecAndPrepare(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteBadCmp || resp.RequestID != "retry" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
}

func TestDurableNegativeOnePCOutcomeReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 73, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 4, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteBadCmp {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(0, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	resp, err = restarted.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != sinfonia.VoteBadCmp || resp.RequestID != "retry" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	got, err := restarted.ReadBytesForTest(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0 {
		t.Fatalf("retry applied write after original abort: got %q", got)
	}
}

func TestPrepareCrashAfterRedoBeforeReplayDoesNotReexecuteKnownTID(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after redo")
	n.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
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

	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
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
	if !errors.Is(err, memnode.ErrReplayUnavailable) || resp.Vote != sinfonia.VoteBadInternal {
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

func TestOnePCCrashAfterDecisionBeforeReplayDoesNotReexecuteKnownTID(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after decision")
	n.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
		if phase == "one-pc-before-response" {
			return crash
		}
		return nil
	})
	txId := sinfonia.TransactionID{CoordinatorID: 76, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(ctx, req)
	if !errors.Is(err, crash) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("boundary response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 32, dir)
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
	resp, err = restarted.ExecCommit1PC(ctx, req)
	if !errors.Is(err, memnode.ErrReplayUnavailable) || resp.Vote != sinfonia.VoteBadInternal {
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

func TestOnePCRetryFinishesPendingRecoveryWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: tid, ReadItems: []sinfonia.ReadItem{{Len: 1}}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}
	injected := errors.New("interrupted before reply")
	node.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return injected })
	if _, err := node.ExecCommit1PC(ctx, req); !errors.Is(err, injected) {
		t.Fatalf("initial call: %v", err)
	}
	node.SetOutcomeReplayBoundaryHookForTest(nil)
	node.SetWriteApplyFaultForTest(func(uint64, []byte) error { return injected })
	if _, err := node.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true}); !errors.Is(err, injected) {
		t.Fatalf("recovery: %v", err)
	}
	node.SetWriteApplyFaultForTest(nil)
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
