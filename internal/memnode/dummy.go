package memnode

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

// DummyNode is a stub node for tests that don't need transaction behavior.
type DummyNode struct{}

func (d *DummyNode) Read(_ context.Context, _ uint64, length uint32) ([]byte, error) {
	return make([]byte, length), nil
}

func (d *DummyNode) Write(_ context.Context, _ uint64, _ []byte) error {
	return nil
}

func (d *DummyNode) ExecAndPrepare(_ context.Context, _ sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	return sinfonia.ExecPrepareResponse{Vote: sinfonia.VoteOk}, nil
}

func (d *DummyNode) ExecCommit1PC(_ context.Context, _ sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	return sinfonia.ExecCommit1PCResponse{Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{}}, nil
}

func (d *DummyNode) Commit(_ context.Context, _ sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return sinfonia.CommitResponse{Ack: true}, nil
}

func (d *DummyNode) CommitBatch(_ context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	acks := make([]bool, len(req.Decisions))
	for i := range acks {
		acks[i] = true
	}
	return sinfonia.CommitBatchResponse{Acks: acks}, nil
}
