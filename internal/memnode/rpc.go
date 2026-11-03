package memnode

import (
	"context"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func (n *Participant) RPCServer() *rpcwire.Server {
	srv := rpcwire.NewServer()
	// Admit execution before the participant mutex. Commit and recovery bypass
	// this limit so existing transactions can finish when execution is full.
	srv.RegisterTyped(sinfonia.MethodExecPrepare, func(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
		select {
		case n.executionSlots <- struct{}{}:
			defer func() { <-n.executionSlots }()
		default:
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
		}
		return n.ExecAndPrepare(ctx, req)
	})
	srv.RegisterTyped(sinfonia.MethodExecCommit1PC, func(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
		select {
		case n.executionSlots <- struct{}{}:
			defer func() { <-n.executionSlots }()
		default:
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
		}
		return n.ExecCommit1PC(ctx, req)
	})
	srv.RegisterTyped(sinfonia.MethodCommit, n.Commit)
	srv.RegisterTyped(sinfonia.MethodCommitBatch, n.CommitBatch)
	srv.RegisterTyped(sinfonia.MethodQueryInDoubt, n.QueryInDoubt)
	srv.RegisterTyped(sinfonia.MethodRecoveryForceAbort, n.RecoveryForceAbort)
	srv.RegisterTyped(sinfonia.MethodRecoveryDecide, n.RecoveryDecide)
	srv.RegisterTyped(sinfonia.MethodNotifyApplied, n.NotifyApplied)
	srv.RegisterTyped(sinfonia.MethodRunRedoLogGC, n.RunRedoLogGCRPC)
	srv.RegisterTyped(sinfonia.MethodRunForcedAbortGC, n.RunForcedAbortGCRPC)
	srv.RegisterTyped(sinfonia.MethodQueryTidStatus, n.QueryTidStatus)
	return srv
}
