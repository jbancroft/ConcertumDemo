package client

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/jbancroft/concertum2/internal/proto"
)

type diagnosticNode struct {
	id          uint32
	vote        proto.Vote
	commitAck   bool
	commitErr   error
	mu          *sync.Mutex
	commitOrder *[]uint32
	prepareTIDs *[]proto.TransactionID
}

func (n diagnosticNode) Read(context.Context, uint64, uint32) ([]byte, error) { return nil, nil }
func (n diagnosticNode) Write(context.Context, uint64, []byte) error          { return nil }
func (n diagnosticNode) ExecCommit1PC(context.Context, proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
	return proto.ExecCommit1PCResponse{}, nil
}
func (n diagnosticNode) ExecPrepare(_ context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
	n.mu.Lock()
	*n.prepareTIDs = append(*n.prepareTIDs, req.TID)
	n.mu.Unlock()
	return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: n.vote}, nil
}
func (n diagnosticNode) Commit(_ context.Context, req proto.CommitRequest) (proto.CommitResponse, error) {
	n.mu.Lock()
	*n.commitOrder = append(*n.commitOrder, n.id)
	n.mu.Unlock()
	return proto.CommitResponse{RequestID: req.RequestID, Ack: n.commitAck}, n.commitErr
}
func (n diagnosticNode) CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	acks := make([]bool, 0, len(req.Decisions))
	for _, decision := range req.Decisions {
		resp, err := n.Commit(ctx, decision)
		if err != nil {
			return proto.CommitBatchResponse{}, err
		}
		acks = append(acks, resp.Ack)
	}
	return proto.CommitBatchResponse{Acks: acks}, nil
}

func TestPhaseTwoDiagnosticPinsTIDAndCheckpointsSortedDeliveries(t *testing.T) {
	var mu sync.Mutex
	var commitOrder []uint32
	var prepareTIDs []proto.TransactionID
	node := func(id uint32) diagnosticNode {
		return diagnosticNode{id: id, vote: proto.VoteOK, commitAck: true, mu: &mu, commitOrder: &commitOrder, prepareTIDs: &prepareTIDs}
	}
	coord := NewCoordinator(map[uint32]MemNode{2: node(2), 1: node(1)})
	tid := proto.TransactionID{CoordinatorID: 77, Counter: 9, Epoch: 3}
	var checkpoints []PhaseTwoCheckpoint
	if err := coord.ConfigurePhaseTwoDiagnostic(PhaseTwoDiagnostic{TID: tid, Checkpoint: func(checkpoint PhaseTwoCheckpoint) {
		checkpoints = append(checkpoints, checkpoint)
	}}); err != nil {
		t.Fatal(err)
	}
	result := coord.ExecAndCommit(context.Background(), "diagnostic", nil, nil, []proto.WriteItem{
		{MemID: 2, Addr: 0, Data: []byte("B")}, {MemID: 1, Addr: 0, Data: []byte("A")},
	})
	if !result.Committed || result.UnknownOutcome {
		t.Fatalf("result=%+v", result)
	}
	if got, want := commitOrder, []uint32{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("commit order=%v want=%v", got, want)
	}
	if len(prepareTIDs) != 2 || prepareTIDs[0] != tid || prepareTIDs[1] != tid {
		t.Fatalf("prepare tids=%v want two copies of %+v", prepareTIDs, tid)
	}
	gotDelivered := make([]int, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.TID != tid || checkpoint.Total != 2 {
			t.Fatalf("checkpoint=%+v", checkpoint)
		}
		gotDelivered = append(gotDelivered, checkpoint.Delivered)
	}
	if want := []int{0, 1, 2}; !reflect.DeepEqual(gotDelivered, want) {
		t.Fatalf("checkpoints=%v want=%v", gotDelivered, want)
	}
}

func TestPhaseTwoDiagnosticDoesNotCheckpointNonPositivePrepare(t *testing.T) {
	var mu sync.Mutex
	var commitOrder []uint32
	var prepareTIDs []proto.TransactionID
	n1 := diagnosticNode{id: 1, vote: proto.VoteOK, commitAck: true, mu: &mu, commitOrder: &commitOrder, prepareTIDs: &prepareTIDs}
	n2 := diagnosticNode{id: 2, vote: proto.VoteBadCmp, commitAck: true, mu: &mu, commitOrder: &commitOrder, prepareTIDs: &prepareTIDs}
	coord := NewCoordinator(map[uint32]MemNode{1: n1, 2: n2})
	checkpointed := false
	err := coord.ConfigurePhaseTwoDiagnostic(PhaseTwoDiagnostic{
		TID:        proto.TransactionID{CoordinatorID: 78, Counter: 1, Epoch: 1},
		Checkpoint: func(PhaseTwoCheckpoint) { checkpointed = true },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coord.ExecAndCommit(context.Background(), "diagnostic", nil, nil, []proto.WriteItem{
		{MemID: 1, Addr: 0, Data: []byte("A")}, {MemID: 2, Addr: 0, Data: []byte("B")},
	})
	if result.Committed || result.Status != proto.VoteBadCmp {
		t.Fatalf("result=%+v", result)
	}
	if checkpointed {
		t.Fatal("diagnostic checkpoint ran without an all-positive prepare result")
	}
}

func TestPhaseTwoDiagnosticDoesNotCountUnacknowledgedDelivery(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		ack  bool
	}{{name: "error", err: context.DeadlineExceeded}, {name: "negative_ack", ack: false}} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var commitOrder []uint32
			var prepareTIDs []proto.TransactionID
			node := func(id uint32) diagnosticNode {
				return diagnosticNode{id: id, vote: proto.VoteOK, commitAck: true, mu: &mu, commitOrder: &commitOrder, prepareTIDs: &prepareTIDs}
			}
			first := node(1)
			first.commitAck = test.ack
			first.commitErr = test.err
			coord := NewCoordinator(map[uint32]MemNode{1: first, 2: node(2)})
			var delivered []int
			err := coord.ConfigurePhaseTwoDiagnostic(PhaseTwoDiagnostic{
				TID:        proto.TransactionID{CoordinatorID: 79, Counter: 1, Epoch: 1},
				Checkpoint: func(checkpoint PhaseTwoCheckpoint) { delivered = append(delivered, checkpoint.Delivered) },
			})
			if err != nil {
				t.Fatal(err)
			}
			result := coord.ExecAndCommit(context.Background(), "diagnostic", nil, nil, []proto.WriteItem{
				{MemID: 1, Addr: 0, Data: []byte("A")}, {MemID: 2, Addr: 0, Data: []byte("B")},
			})
			if !result.UnknownOutcome || result.Committed || result.TransportErr == nil {
				t.Fatalf("result=%+v", result)
			}
			if want := []int{0}; !reflect.DeepEqual(delivered, want) {
				t.Fatalf("delivered checkpoints=%v want=%v", delivered, want)
			}
		})
	}
}
