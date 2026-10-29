package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type lost1PCNode struct {
	*memnode.Participant
	dropped bool
}

func (n *lost1PCNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	resp, err := n.Participant.ExecCommit1PC(ctx, req)
	if !n.dropped {
		n.dropped = true
		return sinfonia.ExecCommit1PCResponse{}, errors.New("lost 1pc response")
	}
	return resp, err
}

type lostPrepareNode struct {
	*memnode.Participant
	dropped bool
}

type failingCommitNode struct {
	*memnode.Participant
}

type unacknowledgedFallbackCommitNode struct {
	*memnode.Participant
}

func (n *failingCommitNode) Commit(context.Context, sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return sinfonia.CommitResponse{}, errors.New("commit unavailable")
}

func (n *failingCommitNode) CommitBatch(context.Context, sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	return sinfonia.CommitBatchResponse{}, errors.New("commit batch unavailable")
}

func (n *unacknowledgedFallbackCommitNode) Commit(_ context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, nil
}

func (n *unacknowledgedFallbackCommitNode) CommitBatch(context.Context, sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	return sinfonia.CommitBatchResponse{}, errors.New("commit batch unavailable")
}

func (n *lostPrepareNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	resp, err := n.Participant.ExecAndPrepare(ctx, req)
	if !n.dropped {
		n.dropped = true
		return sinfonia.ExecPrepareResponse{}, errors.New("lost prepare response")
	}
	return resp, err
}

func TestCoordinatorReportsUnknownWhen1PCResponseLostAfterApply(t *testing.T) {
	n := &lost1PCNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{1: n})
	res := coord.ExecuteAndCommit(context.Background(), "", nil, nil, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}})
	if !res.UnknownOutcome || res.TransportErr == nil || res.Committed {
		t.Fatalf("expected unknown outcome with transport error, got %+v", res)
	}
	got, err := n.Read(context.Background(), 0, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("expected applied write despite lost response, got %q err=%v", got, err)
	}
}

func TestCoordinatorReportsUnknownWhenPrepareResponseLostAfterVote(t *testing.T) {
	n1 := &lostPrepareNode{Participant: memnode.NewParticipant(16)}
	n2 := memnode.NewParticipant(16)
	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	coord.coordID = 1
	res := coord.ExecuteAndCommit(context.Background(), "", nil, nil, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}, {MemID: 2, Addr: 0, Data: []byte("B")}})
	if !res.UnknownOutcome || res.TransportErr == nil || res.Committed {
		t.Fatalf("expected unknown outcome with transport error, got %+v", res)
	}
	resp, err := n1.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}, RequestID: "retry", Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}})
	if err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("expected idempotent prepared vote on retry, resp=%+v err=%v", resp, err)
	}
}

func TestCoordinatorIDsAreDistinctAndNonZero(t *testing.T) {
	first := NewCoordinator(nil)
	second := NewCoordinator(nil)
	if first.coordID == 0 || second.coordID == 0 {
		t.Fatalf("coordinator IDs must be nonzero: first=%d second=%d", first.coordID, second.coordID)
	}
	if first.coordID == second.coordID {
		t.Fatalf("coordinator IDs must be distinct: %d", first.coordID)
	}
}

func TestFlushPendingCommitsReportsEarlierTimerDeliveryFailure(t *testing.T) {
	failing := &failingCommitNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{
		1: failing,
		2: memnode.NewParticipant(16),
	})
	res := coord.ExecuteAndCommit(context.Background(), "", nil, nil, []sinfonia.WriteItem{
		{MemID: 1, Addr: 0, Data: []byte("A")},
		{MemID: 2, Addr: 0, Data: []byte("B")},
	})
	if !res.Committed {
		t.Fatalf("expected commit decision before phase-two delivery: %+v", res)
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coord.FlushPendingCommits(ctx); err == nil {
		t.Fatal("expected flush to report the earlier phase-two delivery failure")
	}
}

func TestFlushPendingCommitsReportsUnacknowledgedFallbackDelivery(t *testing.T) {
	unacknowledged := &unacknowledgedFallbackCommitNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{
		1: unacknowledged,
		2: memnode.NewParticipant(16),
	})
	res := coord.ExecuteAndCommit(context.Background(), "", nil, nil, []sinfonia.WriteItem{
		{MemID: 1, Addr: 0, Data: []byte("A")},
		{MemID: 2, Addr: 0, Data: []byte("B")},
	})
	if !res.Committed {
		t.Fatalf("expected commit decision before phase-two delivery: %+v", res)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coord.FlushPendingCommits(ctx); err == nil {
		t.Fatal("expected flush to report the unacknowledged fallback delivery")
	}
}
