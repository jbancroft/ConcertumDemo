package client

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/rpcwire"
)

type RPCMemNode struct {
	rpc        *rpcwire.Client
	generation uint64
}

func NewRPCMemNode(addr string, tlsConfig *tls.Config, timeout time.Duration) *RPCMemNode {
	return NewRPCMemNodeWithGeneration(addr, tlsConfig, timeout, 0)
}

func NewRPCMemNodeWithGeneration(addr string, tlsConfig *tls.Config, timeout time.Duration, generation uint64) *RPCMemNode {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &RPCMemNode{
		rpc:        rpcwire.NewClient(strings.TrimSpace(addr), rpcwire.ClientOptions{TLS: tlsConfig, DialTimeout: timeout}),
		generation: generation,
	}
}

func (n *RPCMemNode) Read(ctx context.Context, addr uint64, length uint32) ([]byte, error) {
	var out proto.ReadResponse
	err := n.rpc.Call(ctx, proto.MethodRead, proto.ReadRequest{Addr: addr, Len: length, Generation: n.generation}, &out)
	if err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (n *RPCMemNode) Write(ctx context.Context, addr uint64, data []byte) error {
	var out proto.WriteResponse
	return n.rpc.Call(ctx, proto.MethodWrite, proto.WriteRequest{Addr: addr, Data: data, Generation: n.generation}, &out)
}

func (n *RPCMemNode) DescribeTransactionCapabilities(ctx context.Context, req proto.DescribeTransactionCapabilitiesRequest) (proto.DescribeTransactionCapabilitiesResponse, error) {
	var out proto.DescribeTransactionCapabilitiesResponse
	return out, n.rpc.Call(ctx, proto.MethodTransactionCapabilities, req, &out)
}

func (n *RPCMemNode) ExecPrepare(ctx context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
	var out proto.ExecPrepareResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodExecPrepare, req, &out)
}

func (n *RPCMemNode) ExecCommit1PC(ctx context.Context, req proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
	var out proto.ExecCommit1PCResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodExecCommit1PC, req, &out)
}

func (n *RPCMemNode) Commit(ctx context.Context, req proto.CommitRequest) (proto.CommitResponse, error) {
	var out proto.CommitResponse
	if req.Generation == 0 {
		req.Generation = n.generation
	}
	return out, n.rpc.Call(ctx, proto.MethodCommit, req, &out)
}

func (n *RPCMemNode) CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	var out proto.CommitBatchResponse
	for i := range req.Decisions {
		if req.Decisions[i].Generation == 0 {
			req.Decisions[i].Generation = n.generation
		}
	}
	return out, n.rpc.Call(ctx, proto.MethodCommitBatch, req, &out)
}
