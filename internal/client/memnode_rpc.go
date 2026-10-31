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

func (n *RPCParticipant) Close() error {
	return n.rpc.Close()
}

func NewRPCParticipant(addr string, tlsConfig *tls.Config, timeout time.Duration) *RPCParticipant {
	return NewRPCParticipantWithGeneration(addr, tlsConfig, timeout, 0)
}

func NewRPCParticipantWithGeneration(addr string, tlsConfig *tls.Config, timeout time.Duration, generation uint64) *RPCParticipant {
	return &RPCParticipant{
		rpc:        rpcwire.NewClient(strings.TrimSpace(addr), rpcwire.ClientOptions{TLS: tlsConfig, DialTimeout: timeout, CallTimeout: timeout}),
		generation: generation,
	}
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

func (p *RPCParticipant) RecoveryForceAbort(ctx context.Context, req sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	var out sinfonia.RecoverForceAbortResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodRecoveryForceAbort, req, &out)
}
func (p *RPCParticipant) RecoveryDecide(ctx context.Context, req sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error) {
	var out sinfonia.RecoverDecideResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodRecoveryDecide, req, &out)
}
