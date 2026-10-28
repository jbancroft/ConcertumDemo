package memnode

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto"
)

// DummyNode is a stub node for tests that don't need transaction behavior.
type DummyNode struct{}

func (d *DummyNode) Read(_ context.Context, _ uint64, length uint32) ([]byte, error) {
	return make([]byte, length), nil
}

func (d *DummyNode) Write(_ context.Context, _ uint64, _ []byte) error {
	return nil
}

func (d *DummyNode) ExecAndPrepare(_ context.Context, _ proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
	return proto.ExecPrepareResponse{Vote: proto.VoteOk}, nil
}

func (d *DummyNode) ExecCommit1PC(_ context.Context, _ proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
	return proto.ExecCommit1PCResponse{Vote: proto.VoteOk, ReadData: map[uint32][]byte{}}, nil
}

func (d *DummyNode) Commit(_ context.Context, _ proto.CommitRequest) (proto.CommitResponse, error) {
	return proto.CommitResponse{Ack: true}, nil
}

func (d *DummyNode) CommitBatch(_ context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	acks := make([]bool, len(req.Decisions))
	for i := range acks {
		acks[i] = true
	}
	return proto.CommitBatchResponse{Acks: acks}, nil
}
