package memnode_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/storage"
)

func TestDurableOnePCReplaysOriginalReadPayloadWithoutReapplyingWrite(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 71, Counter: 1, Epoch: 1}
	req := proto.ExecCommit1PCRequest{
		TID:        tid,
		RequestID:  "first",
		ReadItems:  []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
		WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	resp, err := n.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != proto.VoteOK || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableNode(1, 32, dir)
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
	if err != nil || resp.Vote != proto.VoteOK || resp.RequestID != "retry" || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	got, err := restarted.Read(ctx, 8, 1)
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
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 74, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{
		TID:          tid,
		RequestID:    "first",
		Participants: []uint32{1},
		ReadItems:    []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
	}
	resp, err := n.ExecPrepare(ctx, req)
	if err != nil || resp.Vote != proto.VoteOK || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := restarted.QueryTidStatus(ctx, proto.QueryTidStatusRequest{TID: tid})
	if err != nil || status.KnownDecision || !status.VotedOK {
		t.Fatalf("restarted vote status = %+v, err = %v", status, err)
	}
	if err := restarted.ApplyWriteForTest(0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "retry"
	resp, err = restarted.ExecPrepare(ctx, req)
	if err != nil || resp.Vote != proto.VoteOK || resp.RequestID != "retry" || string(resp.ReadData[0]) != "seed" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
}

func TestDurableNegativePrepareOutcomeReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 72, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{
		TID:          tid,
		RequestID:    "first",
		Participants: []uint32{1},
		CmpItems:     []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}},
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("A")}},
	}
	resp, err := n.ExecPrepare(ctx, req)
	if err != nil || resp.Vote != proto.VoteBadCmp {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableNode(1, 32, dir)
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
	resp, err = restarted.ExecPrepare(ctx, req)
	if err != nil || resp.Vote != proto.VoteBadCmp || resp.RequestID != "retry" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
}

func TestDurableNegativeOnePCOutcomeReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 73, Counter: 1, Epoch: 1}
	req := proto.ExecCommit1PCRequest{TID: tid, RequestID: "first", CmpItems: []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(ctx, req)
	if err != nil || resp.Vote != proto.VoteBadCmp {
		t.Fatalf("first response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableNode(1, 32, dir)
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
	if err != nil || resp.Vote != proto.VoteBadCmp || resp.RequestID != "retry" {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	got, err := restarted.Read(ctx, 4, 1)
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
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after redo")
	n.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ proto.TransactionID) error {
		if phase == "prepare-before-response" {
			return crash
		}
		return nil
	})
	tid := proto.TransactionID{CoordinatorID: 75, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{
		TID:          tid,
		RequestID:    "first",
		Participants: []uint32{1},
		ReadItems:    []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}},
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	resp, err := n.ExecPrepare(ctx, req)
	if !errors.Is(err, crash) || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("boundary response = %+v, err = %v", resp, err)
	}
	if got := n.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("redo entries before restart = %d, want 1", got)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableNode(1, 32, dir)
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
	resp, err = restarted.ExecPrepare(ctx, req)
	if !errors.Is(err, memnode.ErrReplayUnavailable) || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	if got := restarted.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("retry appended redo: entries = %d, want 1", got)
	}
	got, err := restarted.Read(ctx, 8, 1)
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
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after decision")
	n.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ proto.TransactionID) error {
		if phase == "one-pc-before-response" {
			return crash
		}
		return nil
	})
	tid := proto.TransactionID{CoordinatorID: 76, Counter: 1, Epoch: 1}
	req := proto.ExecCommit1PCRequest{TID: tid, RequestID: "first", WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(ctx, req)
	if !errors.Is(err, crash) || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("boundary response = %+v, err = %v", resp, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableNode(1, 32, dir)
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
	if !errors.Is(err, memnode.ErrReplayUnavailable) || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("retry response = %+v, err = %v", resp, err)
	}
	if got := restarted.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("retry appended redo: entries = %d, want 1", got)
	}
	got, err := restarted.Read(ctx, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("retry reapplied write: got %q, want Z", got)
	}
}

func TestOnePCOutOfBoundsWriteDoesNotPersistSuccess(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableNode(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	tid := proto.TransactionID{CoordinatorID: 77, Counter: 1, Epoch: 1}
	resp, err := n.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{TID: tid, RequestID: "invalid", WriteItems: []proto.WriteItem{{MemID: 1, Addr: 31, Data: []byte("XX")}}})
	if !errors.Is(err, memnode.ErrOutOfBounds) || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("out-of-bounds response = %+v, err = %v", resp, err)
	}
	if n.RedoEntryCountForTest() != 0 || n.HasDecidedForTest(tid) {
		t.Fatal("out-of-bounds request created durable transaction state")
	}
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if replay, ok := store.GetOnePC(tid); ok {
		t.Fatalf("out-of-bounds request persisted success replay: %+v", replay)
	}
}

type blockingReplica struct {
	redoStarted chan struct{}
	releaseRedo chan struct{}
	redoAck     bool
	redoErr     error
}

func newBlockingReplica() *blockingReplica {
	return &blockingReplica{redoStarted: make(chan struct{}, 1), releaseRedo: make(chan struct{}), redoAck: true}
}

func (r *blockingReplica) ReplicateRedoEntry(context.Context, proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	r.redoStarted <- struct{}{}
	<-r.releaseRedo
	return proto.ReplicateRedoEntryResponse{Ack: r.redoAck}, r.redoErr
}

func (r *blockingReplica) ReplicateCommitDecision(context.Context, proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	return proto.ReplicateCommitDecisionResponse{Ack: true}, nil
}

func (r *blockingReplica) ReplicateReplayRecord(context.Context, proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	return proto.ReplicateReplayRecordResponse{Ack: true}, nil
}

func TestReplicatedPrepareDuplicateWaitsForOriginalReadPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableNode(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	replica := newBlockingReplica()
	n.ConfigureAsPrimary(2, replica)
	tid := proto.TransactionID{CoordinatorID: 78, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{TID: tid, RequestID: "first", Participants: []uint32{1}, ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp proto.ExecPrepareResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecPrepare(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original prepare did not reach blocking replica")
	}
	retry := req
	retry.RequestID = "retry"
	duplicate := make(chan result, 1)
	go func() { resp, err := n.ExecPrepare(ctx, retry); duplicate <- result{resp, err} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before original completed: %+v err=%v", got.resp, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(replica.releaseRedo)
	a := <-first
	b := <-duplicate
	if a.err != nil || b.err != nil || a.resp.Vote != proto.VoteOK || b.resp.Vote != proto.VoteOK || string(a.resp.ReadData[0]) != "seed" || string(b.resp.ReadData[0]) != "seed" || b.resp.RequestID != "retry" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
	}
}

func TestReplicatedOnePCDuplicateWaitsForOriginalOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableNode(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	replica := newBlockingReplica()
	n.ConfigureAsPrimary(2, replica)
	tid := proto.TransactionID{CoordinatorID: 79, Counter: 1, Epoch: 1}
	req := proto.ExecCommit1PCRequest{TID: tid, RequestID: "first", ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp proto.ExecCommit1PCResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecCommit1PC(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original 1PC did not reach blocking replica")
	}
	retry := proto.ExecCommit1PCRequest{TID: tid, RequestID: "retry", ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("B")}}}
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
	if a.err != nil || b.err != nil || a.resp.Vote != proto.VoteOK || b.resp.Vote != proto.VoteOK || b.resp.RequestID != "retry" || string(a.resp.ReadData[0]) != "\x00" || string(b.resp.ReadData[0]) != "\x00" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
	}
	got, err := n.Read(ctx, 8, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("write=%q err=%v, want original A", got, err)
	}
}

func TestReplicatedPrepareDuplicateSharesOriginalReplicationFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := memnode.NewDurableNode(1, 32, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	replica := newBlockingReplica()
	replica.redoAck = false
	replica.redoErr = context.DeadlineExceeded
	n.ConfigureAsPrimary(2, replica)
	tid := proto.TransactionID{CoordinatorID: 80, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{TID: tid, RequestID: "first", Participants: []uint32{1}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
	type result struct {
		resp proto.ExecPrepareResponse
		err  error
	}
	first := make(chan result, 1)
	go func() { resp, err := n.ExecPrepare(ctx, req); first <- result{resp, err} }()
	select {
	case <-replica.redoStarted:
	case <-ctx.Done():
		t.Fatal("original prepare did not reach blocking replica")
	}
	retry := req
	retry.RequestID = "retry"
	duplicate := make(chan result, 1)
	go func() { resp, err := n.ExecPrepare(ctx, retry); duplicate <- result{resp, err} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before failed original completed: %+v err=%v", got.resp, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(replica.releaseRedo)
	a := <-first
	b := <-duplicate
	if a.err != nil || b.err != nil || a.resp.Vote != proto.VoteBadInternal || b.resp.Vote != proto.VoteBadInternal || a.resp.RequestID != "first" || b.resp.RequestID != "retry" {
		t.Fatalf("original=%+v err=%v duplicate=%+v err=%v", a.resp, a.err, b.resp, b.err)
	}
}
