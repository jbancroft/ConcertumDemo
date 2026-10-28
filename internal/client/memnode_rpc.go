package client

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

type RPCParticipant struct {
	rpc        *rpcwire.Client
	generation uint64
}

func NewRPCParticipant(addr string, tlsConfig *tls.Config, timeout time.Duration) *RPCParticipant {
	return NewRPCParticipantWithGeneration(addr, tlsConfig, timeout, 0)
}

func NewRPCParticipantWithGeneration(addr string, tlsConfig *tls.Config, timeout time.Duration, generation uint64) *RPCParticipant {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &RPCParticipant{
		rpc:        rpcwire.NewClient(strings.TrimSpace(addr), rpcwire.ClientOptions{TLS: tlsConfig, DialTimeout: timeout}),
		generation: generation,
	}
}

func (n *RPCParticipant) Read(ctx context.Context, addr uint64, length uint32) ([]byte, error) {
	var out proto.ReadResponse
	err := n.rpc.Call(ctx, proto.MethodRead, proto.ReadRequest{Addr: addr, Len: length, Generation: n.generation}, &out)
	if err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (n *RPCParticipant) Write(ctx context.Context, addr uint64, data []byte) error {
	var out proto.WriteResponse
	return n.rpc.Call(ctx, proto.MethodWrite, proto.WriteRequest{Addr: addr, Data: data, Generation: n.generation}, &out)
}

func (n *RPCParticipant) ExecAndPrepare(ctx context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
	var out proto.ExecPrepareResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodExecPrepare, req, &out)
}

func (n *RPCParticipant) ExecCommit1PC(ctx context.Context, req proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
	var out proto.ExecCommit1PCResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodExecCommit1PC, req, &out)
}

func (n *RPCParticipant) Commit(ctx context.Context, req proto.CommitRequest) (proto.CommitResponse, error) {
	var out proto.CommitResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodCommit, req, &out)
}

func (n *RPCParticipant) CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	var out proto.CommitBatchResponse
	for i := range req.Decisions {
		if req.Decisions[i].Generation == 0 {
			req.Decisions[i].Generation = n.generation
		}
	}
	return out, n.rpc.Call(ctx, proto.MethodCommitBatch, req, &out)
}
