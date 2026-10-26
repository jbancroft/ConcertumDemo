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

	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/rpcwire"
)

type RPCPeer struct {
	rpc   *rpcwire.Client
	owner string
}

func (p *RPCPeer) DescribeTransactionCapabilities(ctx context.Context, req proto.DescribeTransactionCapabilitiesRequest) (proto.DescribeTransactionCapabilitiesResponse, error) {
	var out proto.DescribeTransactionCapabilitiesResponse
	return out, p.rpc.Call(ctx, proto.MethodTransactionCapabilities, req, &out)
}

func NewRPCPeer(addr string, tlsConfig *tls.Config, timeout time.Duration) *RPCPeer {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	addr = strings.TrimSpace(addr)
	owner := fmt.Sprintf("rpcpeer/%s", addr)
	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err == nil {
		owner += "/" + hex.EncodeToString(raw[:])
	}
	return &RPCPeer{rpc: rpcwire.NewClient(addr, rpcwire.ClientOptions{TLS: tlsConfig, DialTimeout: timeout}), owner: owner}
}

func (p *RPCPeer) ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	var out proto.ReplicateRedoEntryResponse
	return out, p.rpc.Call(ctx, proto.MethodReplicateRedoEntry, req, &out)
}

func (p *RPCPeer) LockAllBlocking(ctx context.Context) error {
	return p.LockAllBlockingOwned(ctx, p.owner)
}

func (p *RPCPeer) LockAllBlockingOwned(ctx context.Context, owner string) error {
	var out proto.BackupLockAllResponse
	if err := p.rpc.Call(ctx, proto.MethodBackupLockAll, proto.BackupLockAllRequest{Owner: owner}, &out); err != nil {
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
	var out proto.BackupUnlockAllResponse
	_ = p.rpc.Call(context.Background(), proto.MethodBackupUnlockAll, proto.BackupUnlockAllRequest{Owner: owner}, &out)
}

func (p *RPCPeer) RenewAllOwned(ctx context.Context, owner string) error {
	var out proto.BackupRenewLockResponse
	if err := p.rpc.Call(ctx, proto.MethodBackupRenewLock, proto.BackupRenewLockRequest{Owner: owner}, &out); err != nil {
		return err
	}
	if !out.Ack {
		return errors.New("global barrier lease renewal not acknowledged")
	}
	return nil
}

func (p *RPCPeer) ExportReplicationBaseline(ctx context.Context, req proto.ExportReplicationBaselineRequest) (proto.ExportReplicationBaselineResponse, error) {
	var out proto.ExportReplicationBaselineResponse
	return out, p.rpc.Call(ctx, proto.MethodExportReplicationBaseline, req, &out)
}

func (p *RPCPeer) ExportReplicationBaselineChunk(ctx context.Context, req proto.ExportReplicationBaselineChunkRequest) (proto.ExportReplicationBaselineChunkResponse, error) {
	var out proto.ExportReplicationBaselineChunkResponse
	return out, p.rpc.Call(ctx, proto.MethodExportReplicationBaselineChunk, req, &out)
}

func (p *RPCPeer) ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	var out proto.ReplicateCommitDecisionResponse
	return out, p.rpc.Call(ctx, proto.MethodReplicateCommitDecision, req, &out)
}

func (p *RPCPeer) ReplicateReplayRecord(ctx context.Context, req proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	var out proto.ReplicateReplayRecordResponse
	return out, p.rpc.Call(ctx, proto.MethodReplicateReplayRecord, req, &out)
}

func (p *RPCPeer) ListCommittedTIDs(ctx context.Context, req proto.ListCommittedTIDsRequest) (proto.ListCommittedTIDsResponse, error) {
	var out proto.ListCommittedTIDsResponse
	return out, p.rpc.Call(ctx, proto.MethodListCommittedTIDs, req, &out)
}

func (p *RPCPeer) ListRedoEntries(ctx context.Context, req proto.ListRedoEntriesRequest) (proto.ListRedoEntriesResponse, error) {
	var out proto.ListRedoEntriesResponse
	return out, p.rpc.Call(ctx, proto.MethodListRedoEntries, req, &out)
}

func (p *RPCPeer) NotifyApplied(ctx context.Context, req proto.NotifyAppliedRequest) (proto.NotifyAppliedResponse, error) {
	var out proto.NotifyAppliedResponse
	return out, p.rpc.Call(ctx, proto.MethodNotifyApplied, req, &out)
}

func (p *RPCPeer) QueryTidStatus(ctx context.Context, req proto.QueryTidStatusRequest) (proto.QueryTidStatusResponse, error) {
	var out proto.QueryTidStatusResponse
	return out, p.rpc.Call(ctx, proto.MethodQueryTidStatus, req, &out)
}
