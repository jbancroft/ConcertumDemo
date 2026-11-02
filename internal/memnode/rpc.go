package memnode

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func (n *Participant) requireRecoveryComplete() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return n.restartRequired
	}
	if !n.recoveryComplete {
		return ErrRecoveryPending
	}
	return nil
}

func (n *Participant) RPCServer() *rpcwire.Server {
	srv := rpcwire.NewServer(nil)
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
	rpcwire.RegisterTyped(srv, sinfonia.MethodNotifyApplied, n.NotifyApplied)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRunRedoLogGC, n.RunRedoLogGCRPC)
	rpcwire.RegisterTyped(srv, sinfonia.MethodRunForcedAbortGC, n.RunForcedAbortGCRPC)
	rpcwire.RegisterTyped(srv, sinfonia.MethodQueryTidStatus, n.QueryTidStatus)
	return srv
}
