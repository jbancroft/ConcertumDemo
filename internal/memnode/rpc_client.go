package memnode

import (
	"context"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

const (
	defaultPeerTimeout = 5 * time.Second
)

type RPCPeer struct {
	rpc *rpcwire.Client
}

func NewRPCPeer(addr string, timeout time.Duration) *RPCPeer {
	if timeout <= 0 {
		timeout = defaultPeerTimeout
	}
	addr = strings.TrimSpace(addr)
	return &RPCPeer{rpc: rpcwire.NewClient(addr, rpcwire.ClientOptions{DialTimeout: timeout, CallTimeout: timeout})}
}

func (p *RPCPeer) NotifyApplied(ctx context.Context, req sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	var out sinfonia.NotifyAppliedResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodNotifyApplied, req, &out)
}

func (p *RPCPeer) QueryTidStatus(ctx context.Context, req sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error) {
	var out sinfonia.QueryTidStatusResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodQueryTidStatus, req, &out)
}

func (p *RPCPeer) RecoveryForceAbort(ctx context.Context, req sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	var out sinfonia.RecoverForceAbortResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodRecoveryForceAbort, req, &out)
}
func (p *RPCPeer) RecoveryDecide(ctx context.Context, req sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error) {
	var out sinfonia.RecoverDecideResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodRecoveryDecide, req, &out)
}
