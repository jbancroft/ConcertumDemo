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

func TestReplicatedPrepareDuplicateWaitsForOriginalReadPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableParticipant(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	replica := newBlockingReplica()
	n.ConfigureAsPrimary(2, replica)
	txId := sinfonia.TransactionID{CoordinatorID: 78, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecPrepareRequest{TID: txId, RequestID: "first", Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp sinfonia.ExecPrepareResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecAndPrepare(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original prepare did not reach blocking replica")
	}
	retry := req
	retry.RequestID = "retry"
	duplicate := make(chan result, 1)
	go func() { resp, err := n.ExecAndPrepare(ctx, retry); duplicate <- result{resp, err} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before original completed: %+v err=%v", got.resp, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(replica.releaseRedo)
	a := <-first
	b := <-duplicate
	if a.err != nil || b.err != nil || a.resp.Vote != sinfonia.VoteOk || b.resp.Vote != sinfonia.VoteOk || string(a.resp.ReadData[0]) != "seed" || string(b.resp.ReadData[0]) != "seed" || b.resp.RequestID != "retry" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
	}
}

func TestReplicatedOnePCDuplicateWaitsForOriginalOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableParticipant(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	replica := newBlockingReplica()
	n.ConfigureAsPrimary(2, replica)
	txId := sinfonia.TransactionID{CoordinatorID: 79, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp sinfonia.ExecCommit1PCResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecCommit1PC(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original 1PC did not reach blocking replica")
	}
	retry := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "retry", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("B")}}}
	duplicate := make(chan result, 1)
	go func() { resp, err := n.ExecCommit1PC(ctx, retry); duplicate <- result{resp, err} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before original completed: %+v err=%v", got.resp, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(replica.releaseRedo)
	a := <-first
	b := <-duplicate
	if a.err != nil || b.err != nil || a.resp.Vote != sinfonia.VoteOk || b.resp.Vote != sinfonia.VoteOk || b.resp.RequestID != "retry" || string(a.resp.ReadData[0]) != "\x00" || string(b.resp.ReadData[0]) != "\x00" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
	}
	got, err := n.ReadBytesForTest(8, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("write=%q err=%v, want original A", got, err)
	}
}

func TestReplicatedPrepareDuplicateSharesOriginalReplicationFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableParticipant(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	replica := newBlockingReplica()
	replica.redoAck = false
	replica.redoErr = context.DeadlineExceeded
	n.ConfigureAsPrimary(2, replica)
	txId := sinfonia.TransactionID{CoordinatorID: 80, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecPrepareRequest{TID: txId, RequestID: "first", Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp sinfonia.ExecPrepareResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecAndPrepare(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original prepare did not reach blocking replica")
	}
	retry := req
	retry.RequestID = "retry"
	duplicate := make(chan result, 1)
	go func() { resp, err := n.ExecAndPrepare(ctx, retry); duplicate <- result{resp, err} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before failed original completed: %+v err=%v", got.resp, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(replica.releaseRedo)
	a := <-first
	b := <-duplicate
	if !errors.Is(a.err, context.DeadlineExceeded) || !errors.Is(b.err, context.DeadlineExceeded) || a.resp.Vote != sinfonia.VoteBadInternal || b.resp.Vote != sinfonia.VoteBadInternal || a.resp.RequestID != "first" || b.resp.RequestID != "retry" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
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
