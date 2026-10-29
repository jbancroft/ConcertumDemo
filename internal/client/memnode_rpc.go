package client

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
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
	var out sinfonia.ReadResponse
	err := n.rpc.Call(ctx, sinfonia.MethodRead, sinfonia.ReadRequest{Addr: addr, Len: length, Generation: n.generation}, &out)
	if err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (n *RPCParticipant) Write(ctx context.Context, addr uint64, data []byte) error {
	var out sinfonia.WriteResponse
	return n.rpc.Call(ctx, sinfonia.MethodWrite, sinfonia.WriteRequest{Addr: addr, Data: data, Generation: n.generation}, &out)
}

func (n *RPCParticipant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	var out sinfonia.ExecPrepareResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, sinfonia.MethodExecPrepare, req, &out)
}

func (n *RPCParticipant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	var out sinfonia.ExecCommit1PCResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, sinfonia.MethodExecCommit1PC, req, &out)
}

func (n *RPCParticipant) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	var out sinfonia.CommitResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, sinfonia.MethodCommit, req, &out)
}

func (n *RPCParticipant) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	var out sinfonia.CommitBatchResponse
	for i := range req.Decisions {
		if req.Decisions[i].Generation == 0 {
			req.Decisions[i].Generation = n.generation
		}
	}
	return out, n.rpc.Call(ctx, sinfonia.MethodCommitBatch, req, &out)
}
