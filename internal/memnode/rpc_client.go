package memnode

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

const (
	defaultPeerTimeout  = 5 * time.Second
	rpcPeerOwnerIDBytes = 16
)

type RPCPeer struct {
	rpc   *rpcwire.Client
	owner string
}

func NewRPCPeer(addr string, tlsConfig *tls.Config, timeout time.Duration) *RPCPeer {
	if timeout <= 0 {
		timeout = defaultPeerTimeout
	}
	addr = strings.TrimSpace(addr)
	owner := fmt.Sprintf("rpcpeer/%s", addr)
	var raw [rpcPeerOwnerIDBytes]byte
	if _, err := cryptorand.Read(raw[:]); err == nil {
		owner += "/" + hex.EncodeToString(raw[:])
	}
	return &RPCPeer{rpc: rpcwire.NewClient(addr, rpcwire.ClientOptions{TLS: tlsConfig, DialTimeout: timeout, CallTimeout: timeout}), owner: owner}
}

func (p *RPCPeer) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	var out sinfonia.ReplicateRedoEntryResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodReplicateRedoEntry, req, &out)
}

func (p *RPCPeer) LockAllBlocking(ctx context.Context) error {
	return p.LockAllBlockingOwned(ctx, p.owner)
}

func (p *RPCPeer) LockAllBlockingOwned(ctx context.Context, owner string) error {
	var out sinfonia.BackupLockAllResponse
	if err := p.rpc.Call(ctx, sinfonia.MethodBackupLockAll, sinfonia.BackupLockAllRequest{Owner: owner}, &out); err != nil {
		return err
	}
	if !out.Ack {
		return errors.New("primary backup lock not acknowledged")
	}
	return nil
}

func (p *RPCPeer) UnlockAll() {
	p.UnlockAllOwned(p.owner)
}

func (p *RPCPeer) UnlockAllOwned(owner string) {
	var out sinfonia.BackupUnlockAllResponse
	_ = p.rpc.Call(context.Background(), sinfonia.MethodBackupUnlockAll, sinfonia.BackupUnlockAllRequest{Owner: owner}, &out)
}

func (p *RPCPeer) RenewAllOwned(ctx context.Context, owner string) error {
	var out sinfonia.BackupRenewLockResponse
	if err := p.rpc.Call(ctx, sinfonia.MethodBackupRenewLock, sinfonia.BackupRenewLockRequest{Owner: owner}, &out); err != nil {
		return err
	}
	if !out.Ack {
		return errors.New("global barrier lease renewal not acknowledged")
	}
	return nil
}

func (p *RPCPeer) ExportReplicationBaseline(ctx context.Context, req sinfonia.ExportReplicationBaselineRequest) (sinfonia.ExportReplicationBaselineResponse, error) {
	var out sinfonia.ExportReplicationBaselineResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodExportReplicationBaseline, req, &out)
}

func (p *RPCPeer) ExportReplicationBaselineChunk(ctx context.Context, req sinfonia.ExportReplicationBaselineChunkRequest) (sinfonia.ExportReplicationBaselineChunkResponse, error) {
	var out sinfonia.ExportReplicationBaselineChunkResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodExportReplicationBaselineChunk, req, &out)
}

func (p *RPCPeer) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	var out sinfonia.ReplicateCommitDecisionResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodReplicateCommitDecision, req, &out)
}

func (p *RPCPeer) ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	var out sinfonia.ReplicateReplayRecordResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodReplicateReplayRecord, req, &out)
}

func (p *RPCPeer) ListCommittedTIDs(ctx context.Context, req sinfonia.ListCommittedTIDsRequest) (sinfonia.ListCommittedTIDsResponse, error) {
	var out sinfonia.ListCommittedTIDsResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodListCommittedTIDs, req, &out)
}

func (p *RPCPeer) ListRedoEntries(ctx context.Context, req sinfonia.ListRedoEntriesRequest) (sinfonia.ListRedoEntriesResponse, error) {
	var out sinfonia.ListRedoEntriesResponse
	return out, p.rpc.Call(ctx, sinfonia.MethodListRedoEntries, req, &out)
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
