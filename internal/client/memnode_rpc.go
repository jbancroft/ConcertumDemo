package client

import (
	"context"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

type RPCParticipant struct {
	rpc *rpcwire.Client
}

func (n *RPCParticipant) Close() error {
	return n.rpc.Close()
}

func NewRPCParticipant(addr string, timeout time.Duration) *RPCParticipant {
	return &RPCParticipant{
		rpc: rpcwire.NewClient(strings.TrimSpace(addr), rpcwire.ClientOptions{DialTimeout: timeout, CallTimeout: timeout}),
	}
}

func (n *RPCParticipant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	var out sinfonia.ExecPrepareResponse

	return out, n.rpc.Call(ctx, sinfonia.MethodExecPrepare, req, &out)
}

func (n *RPCParticipant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	var out sinfonia.ExecCommit1PCResponse

	return out, n.rpc.Call(ctx, sinfonia.MethodExecCommit1PC, req, &out)
}

func (n *RPCParticipant) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	var out sinfonia.CommitResponse

	return out, n.rpc.Call(ctx, sinfonia.MethodCommit, req, &out)
}

func (n *RPCParticipant) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	var out sinfonia.CommitBatchResponse
	return out, n.rpc.Call(ctx, sinfonia.MethodCommitBatch, req, &out)
}
