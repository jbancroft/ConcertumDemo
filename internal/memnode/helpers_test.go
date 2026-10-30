package memnode_test

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

// Replica gate shared by the replay and persistence tests.

type blockingReplica struct {
	redoStarted chan struct{}
	releaseRedo chan struct{}
	redoAck     bool
	redoErr     error
}

func newBlockingReplica() *blockingReplica {
	return &blockingReplica{redoStarted: make(chan struct{}, 1), releaseRedo: make(chan struct{}), redoAck: true}
}

func (r *blockingReplica) ReplicateRedoEntry(context.Context, sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	r.redoStarted <- struct{}{}
	<-r.releaseRedo
	return sinfonia.ReplicateRedoEntryResponse{Ack: r.redoAck}, r.redoErr
}

func (r *blockingReplica) ReplicateCommitDecision(context.Context, sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	return sinfonia.ReplicateCommitDecisionResponse{Ack: true}, nil
}

func (r *blockingReplica) ReplicateReplayRecord(context.Context, sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	return sinfonia.ReplicateReplayRecordResponse{Ack: true}, nil
}
