package memnode

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/jbancroft/concertum2/internal/auth"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/rpcwire"
)

func (n *Node) requireRecoveryComplete() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.recoveryComplete {
		return ErrRecoveryPending
	}
	return nil
}

func (n *Node) RPCServer() *rpcwire.Server {
	srv := rpcwire.NewServer(nil, n.rpcConnContext)
	rpcwire.RegisterTyped(srv, proto.MethodRead, func(ctx context.Context, req proto.ReadRequest) (proto.ReadResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return proto.ReadResponse{}, err
		}
		data, err := n.Read(proto.WithGeneration(ctx, req.Generation), req.Addr, req.Len)
		return proto.ReadResponse{Data: data}, err
	})
	rpcwire.RegisterTyped(srv, proto.MethodWrite, func(ctx context.Context, req proto.WriteRequest) (proto.WriteResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return proto.WriteResponse{Ack: false}, err
		}
		err := n.Write(proto.WithGeneration(ctx, req.Generation), req.Addr, req.Data)
		return proto.WriteResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, proto.MethodTransactionCapabilities, n.DescribeTransactionCapabilities)
	rpcwire.RegisterTyped(srv, proto.MethodExecPrepare, func(ctx context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
		}
		return n.ExecPrepare(ctx, req)
	})
	rpcwire.RegisterTyped(srv, proto.MethodExecCommit1PC, func(ctx context.Context, req proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
		}
		return n.ExecCommit1PC(ctx, req)
	})
	rpcwire.RegisterTyped(srv, proto.MethodCommit, n.Commit)
	rpcwire.RegisterTyped(srv, proto.MethodCommitBatch, n.CommitBatch)
	rpcwire.RegisterTyped(srv, proto.MethodQueryInDoubt, n.QueryInDoubt)
	rpcwire.RegisterTyped(srv, proto.MethodRecoveryForceAbort, n.RecoveryForceAbort)
	rpcwire.RegisterTyped(srv, proto.MethodRecoveryDecide, n.RecoveryDecide)
	rpcwire.RegisterTyped(srv, proto.MethodBackupLockAll, func(ctx context.Context, req proto.BackupLockAllRequest) (proto.BackupLockAllResponse, error) {
		err := n.LockAllBlockingOwned(ctx, req.Owner)
		return proto.BackupLockAllResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, proto.MethodBackupRenewLock, func(ctx context.Context, req proto.BackupRenewLockRequest) (proto.BackupRenewLockResponse, error) {
		err := n.RenewAllOwned(ctx, req.Owner)
		return proto.BackupRenewLockResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, proto.MethodBackupUnlockAll, func(ctx context.Context, req proto.BackupUnlockAllRequest) (proto.BackupUnlockAllResponse, error) {
		if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
			return proto.BackupUnlockAllResponse{Ack: false}, err
		}
		n.UnlockAllOwned(req.Owner)
		return proto.BackupUnlockAllResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(srv, proto.MethodBeginBackupPhase, n.BeginBackupPhase)
	rpcwire.RegisterTyped(srv, proto.MethodCompleteBackupPhase, n.CompleteBackupPhase)
	rpcwire.RegisterTyped(srv, proto.MethodExportReplicationBaseline, n.ExportReplicationBaseline)
	rpcwire.RegisterTyped(srv, proto.MethodExportReplicationBaselineChunk, n.ExportReplicationBaselineChunk)
	rpcwire.RegisterTyped(srv, proto.MethodInstallReplicationBaseline, n.InstallReplicationBaseline)
	rpcwire.RegisterTyped(srv, proto.MethodBeginReplicationBaseline, n.BeginReplicationBaseline)
	rpcwire.RegisterTyped(srv, proto.MethodWriteReplicationBaselineChunk, n.WriteReplicationBaselineChunk)
	rpcwire.RegisterTyped(srv, proto.MethodCommitReplicationBaseline, n.CommitReplicationBaseline)
	rpcwire.RegisterTyped(srv, proto.MethodAbortReplicationBaseline, n.AbortReplicationBaseline)
	rpcwire.RegisterTyped(srv, proto.MethodReplicateRedoEntry, n.ReplicateRedoEntry)
	rpcwire.RegisterTyped(srv, proto.MethodReplicateCommitDecision, n.ReplicateCommitDecision)
	rpcwire.RegisterTyped(srv, proto.MethodReplicateReplayRecord, n.ReplicateReplayRecord)
	rpcwire.RegisterTyped(srv, proto.MethodListCommittedTIDs, n.ListCommittedTIDs)
	rpcwire.RegisterTyped(srv, proto.MethodListRedoEntries, n.ListRedoEntries)
	rpcwire.RegisterTyped(srv, proto.MethodNotifyApplied, n.NotifyApplied)
	rpcwire.RegisterTyped(srv, proto.MethodRunRedoLogGC, n.RunRedoLogGCRPC)
	rpcwire.RegisterTyped(srv, proto.MethodRunForcedAbortGC, n.RunForcedAbortGCRPC)
	rpcwire.RegisterTyped(srv, proto.MethodQueryTidStatus, n.QueryTidStatus)
	rpcwire.RegisterTyped(srv, proto.MethodConfigureReplication, n.ConfigureReplication)
	rpcwire.RegisterTyped(srv, proto.MethodReplicationStatus, n.ReplicationStatus)
	return srv
}

func (n *Node) rpcConnContext(conn net.Conn) context.Context {
	ctx := context.Background()
	if n.authorizer == nil {
		return ctx
	}
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return ctx
	}
	state := tlsConn.ConnectionState()
	id, err := n.authorizer.AuthenticateTLS(&state)
	if err != nil {
		return ctx
	}
	return auth.WithIdentity(ctx, id)
}
