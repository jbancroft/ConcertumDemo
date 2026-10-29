package memnode

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func (n *Participant) requireRecoveryComplete() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.recoveryComplete {
		return ErrRecoveryPending
	}
	return nil
}

func (n *Participant) RPCServer() *rpcwire.Server {
	srv := rpcwire.NewServer(nil, nil)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRead, func(ctx context.Context, req sinfonia.ReadRequest) (sinfonia.ReadResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return sinfonia.ReadResponse{}, err
		}
		data, err := n.Read(sinfonia.WithGeneration(ctx, req.Generation), req.Addr, req.Len)
		return sinfonia.ReadResponse{Data: data}, err
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodWrite, func(ctx context.Context, req sinfonia.WriteRequest) (sinfonia.WriteResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return sinfonia.WriteResponse{Ack: false}, err
		}
		err := n.Write(sinfonia.WithGeneration(ctx, req.Generation), req.Addr, req.Data)
		return sinfonia.WriteResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodExecPrepare, func(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
		}
		return n.ExecAndPrepare(ctx, req)
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodExecCommit1PC, func(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
		if err := n.requireRecoveryComplete(); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
		}
		return n.ExecCommit1PC(ctx, req)
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodCommit, n.Commit)
	rpcwire.RegisterTyped(srv, sinfonia.MethodCommitBatch, n.CommitBatch)
	rpcwire.RegisterTyped(srv, sinfonia.MethodQueryInDoubt, n.QueryInDoubt)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRecoveryForceAbort, n.RecoveryForceAbort)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRecoveryDecide, n.RecoveryDecide)
	rpcwire.RegisterTyped(srv, sinfonia.MethodBackupLockAll, func(ctx context.Context, req sinfonia.BackupLockAllRequest) (sinfonia.BackupLockAllResponse, error) {
		err := n.LockAllBlockingOwned(ctx, req.Owner)
		return sinfonia.BackupLockAllResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodBackupRenewLock, func(ctx context.Context, req sinfonia.BackupRenewLockRequest) (sinfonia.BackupRenewLockResponse, error) {
		err := n.RenewAllOwned(ctx, req.Owner)
		return sinfonia.BackupRenewLockResponse{Ack: err == nil}, err
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodBackupUnlockAll, func(ctx context.Context, req sinfonia.BackupUnlockAllRequest) (sinfonia.BackupUnlockAllResponse, error) {
		n.UnlockAllOwned(req.Owner)
		return sinfonia.BackupUnlockAllResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(srv, sinfonia.MethodBeginBackupPhase, n.BeginBackupPhase)
	rpcwire.RegisterTyped(srv, sinfonia.MethodCompleteBackupPhase, n.CompleteBackupPhase)
	rpcwire.RegisterTyped(srv, sinfonia.MethodExportReplicationBaseline, n.ExportReplicationBaseline)
	rpcwire.RegisterTyped(srv, sinfonia.MethodExportReplicationBaselineChunk, n.ExportReplicationBaselineChunk)
	rpcwire.RegisterTyped(srv, sinfonia.MethodInstallReplicationBaseline, n.InstallReplicationBaseline)
	rpcwire.RegisterTyped(srv, sinfonia.MethodBeginReplicationBaseline, n.BeginReplicationBaseline)
	rpcwire.RegisterTyped(srv, sinfonia.MethodWriteReplicationBaselineChunk, n.WriteReplicationBaselineChunk)
	rpcwire.RegisterTyped(srv, sinfonia.MethodCommitReplicationBaseline, n.CommitReplicationBaseline)
	rpcwire.RegisterTyped(srv, sinfonia.MethodAbortReplicationBaseline, n.AbortReplicationBaseline)
	rpcwire.RegisterTyped(srv, sinfonia.MethodReplicateRedoEntry, n.ReplicateRedoEntry)
	rpcwire.RegisterTyped(srv, sinfonia.MethodReplicateCommitDecision, n.ReplicateCommitDecision)
	rpcwire.RegisterTyped(srv, sinfonia.MethodReplicateReplayRecord, n.ReplicateReplayRecord)
	rpcwire.RegisterTyped(srv, sinfonia.MethodListCommittedTIDs, n.ListCommittedTIDs)
	rpcwire.RegisterTyped(srv, sinfonia.MethodListRedoEntries, n.ListRedoEntries)
	rpcwire.RegisterTyped(srv, sinfonia.MethodNotifyApplied, n.NotifyApplied)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRunRedoLogGC, n.RunRedoLogGCRPC)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRunForcedAbortGC, n.RunForcedAbortGCRPC)
	rpcwire.RegisterTyped(srv, sinfonia.MethodQueryTidStatus, n.QueryTidStatus)
	rpcwire.RegisterTyped(srv, sinfonia.MethodConfigureReplication, n.ConfigureReplication)
	rpcwire.RegisterTyped(srv, sinfonia.MethodReplicationStatus, n.ReplicationStatus)
	return srv
}
