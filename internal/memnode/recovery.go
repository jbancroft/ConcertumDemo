package memnode

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrRecoveryPending = errors.New("participant recovery pending peer evidence")

type RecoveryPeer interface {
	recovery.Participant
	NotifyApplied(ctx context.Context, req sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error)
}

func (n *Participant) currentEpochLocked() uint64 {
	n.epoch = max(n.epoch, sinfonia.EpochAt(n.nowFn()))
	return n.epoch
}

func (n *Participant) persistEpochFloorLocked(epoch uint64) error {
	n.epoch = max(n.epoch, epoch)
	if epoch <= n.epochFloor {
		return nil
	}
	if err := storage.StoreEpochFloor(n.storageDir, epoch); err != nil {
		return err
	}
	n.epochFloor = epoch
	return nil
}

func (n *Participant) SetRecoveryPeers(peers map[uint32]RecoveryPeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.participants = maps.Clone(peers)
}

func (n *Participant) QueryTidStatus(ctx context.Context, req sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error) {
	vote, err := n.recoveryVote(ctx, req.TID)
	if err != nil {
		return sinfonia.QueryTidStatusResponse{}, err
	}
	outcome := sinfonia.TidOutcomeUnknown
	if vote.KnownDecision {
		outcome = sinfonia.TidOutcomeAbort
		if vote.DecisionCommit {
			outcome = sinfonia.TidOutcomeCommit
		}
	} else if vote.Vote != sinfonia.VoteOk {
		outcome = sinfonia.TidOutcomeAbort
	}
	return sinfonia.QueryTidStatusResponse{KnownDecision: vote.KnownDecision, Outcome: outcome, VotedOK: vote.Vote == sinfonia.VoteOk}, nil
}

func (n *Participant) RecoveryForceAbort(ctx context.Context, req sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	return n.recoveryVote(ctx, req.TID)
}

// recoveryVote inspects votes under the same mutex as prepare and commit.
// An in-flight request must finish before recovery can decide whether it voted.
func (n *Participant) recoveryVote(ctx context.Context, tid sinfonia.TransactionID) (sinfonia.RecoverForceAbortResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteUnknown}, n.restartRequired
	}
	if err := ctx.Err(); err != nil {
		return sinfonia.RecoverForceAbortResponse{}, err
	}
	out := sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteUnknown}
	// Inspect saved replies without replaying operations.
	one, oneOK := n.onePCOutcomes[tid]
	prep, prepOK := n.prepareOutcomes[tid]
	if oneOK {
		out.Vote = one.resp.Vote
		out.KnownDecision, out.DecisionCommit = true, one.resp.Vote == sinfonia.VoteOk
	} else if prepOK {
		out.Vote = prep.resp.Vote
	}
	if tx := n.inDoubt[tid]; tx != nil {
		out.Vote = sinfonia.VoteOk
	}
	if decision, ok := n.commitOutcomes[tid]; ok {
		out.KnownDecision, out.DecisionCommit = true, decision.actionCommit
	} else if commit, ok := n.decided.Get(tid); ok {
		out.KnownDecision, out.DecisionCommit = true, commit
	}
	if out.KnownDecision {
		out.Vote = sinfonia.VoteBadForced
		if out.DecisionCommit {
			out.Vote = sinfonia.VoteOk
		}
		return out, nil
	}
	if n.forcedAborts.IsForcedAbort(tid) {
		out.Vote = sinfonia.VoteBadForced
		return out, nil
	}
	if _, ok := n.allLogTxIds[tid]; ok {
		out.Vote = sinfonia.VoteOk
	}
	if out.Vote != sinfonia.VoteUnknown {
		return out, nil
	}
	// Collected transactions and expired read-only replies have no retained
	// evidence. Missing old evidence cannot establish an abort.
	if (storage.TransactionRetention{CurrentEpoch: n.currentEpochLocked(), RetryHorizon: storage.TransactionEpochWindow}).Expired(tid) {
		return out, fmt.Errorf("%w: transaction %v has no retained vote", ErrRecoveryPending, tid)
	}
	if err := n.forcedAborts.AddItem(tid); err != nil {
		return out, n.failUncertainLocked("append forced abort", err)
	}
	out.Vote = sinfonia.VoteBadForced
	return out, nil
}

func (n *Participant) RecoveryDecide(ctx context.Context, req sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error) {
	resp, err := n.commit(ctx, sinfonia.CommitRequest{TID: req.TID, ActionCommit: req.ActionCommit}, true)
	if err != nil {
		return sinfonia.RecoverDecideResponse{Ack: false}, err
	}
	return sinfonia.RecoverDecideResponse{Ack: resp.Ack}, nil
}

// Recover resolves logged transactions using saved decisions and peer evidence.
// The node stays unavailable if it can't safely finish recovery.
func (n *Participant) Recover(ctx context.Context) error {
	n.mu.Lock()
	if n.restartRequired != nil {
		err := n.restartRequired
		n.mu.Unlock()
		return err
	}
	n.recoveryComplete = false
	entries := n.redoLog.Entries()
	ptr, err := storage.LoadProcessedPointer(n.storageDir)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	ptr = max(ptr, 0)
	peers := make(map[uint32]recovery.Participant, len(n.participants)+1)
	for k, v := range n.participants {
		peers[k] = v
	}
	peers[n.hostId] = n
	n.mu.Unlock()

	for i := ptr; i < len(entries); i++ {
		entry := entries[i]
		if err := n.callRecoverBoundaryHook("replay-before", entry.TransactionId); err != nil {
			return err
		}
		outcomeCommit := false
		known := false

		n.mu.Lock()
		if n.restartRequired != nil {
			err := n.restartRequired
			n.mu.Unlock()
			return err
		}
		if c, ok := n.decided.Get(entry.TransactionId); ok {
			outcomeCommit = c
			known = true
		}
		n.mu.Unlock()

		if !known {
			result := recovery.Discover(ctx, entry.TransactionId, entry.HostIds, peers)
			if err := ctx.Err(); err != nil {
				return err
			}
			if !result.Known {
				return fmt.Errorf("recover transaction %v: %w", entry.TransactionId, errors.Join(ErrRecoveryPending, result.Err))
			}
			outcomeCommit = result.Committed
		}

		n.mu.Lock()
		if n.restartRequired != nil {
			err := n.restartRequired
			n.mu.Unlock()
			return err
		}
		if decidedCommit, decided := n.decided.Get(entry.TransactionId); decided {
			outcomeCommit = decidedCommit
		} else if err := n.decided.AddItem(entry.TransactionId, outcomeCommit); err != nil {
			err = n.decisionWriteErrorLocked(err)
			n.mu.Unlock()
			return err
		}
		flushNotify := false
		if outcomeCommit {
			if err := n.callRecoverBoundaryHookLocked("apply-before", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
			if err := n.applyWrites(entry.WriteItems); err != nil {
				n.mu.Unlock()
				return err
			}
			n.appliedLocal[entry.TransactionId] = true
			n.enqueueAppliedNotificationsLocked(entry.TransactionId, entry.HostIds)
			flushNotify = true
			if err := n.callRecoverBoundaryHookLocked("apply-after", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		if err := n.callRecoverBoundaryHookLocked("pointer-before", entry.TransactionId); err != nil {
			n.mu.Unlock()
			return err
		}
		// Finish local processing before publishing a checkpoint that can skip
		// this entry on retry, even if publication reports a directory-sync error.
		n.commitOutcomes[entry.TransactionId] = commitOutcome{
			resp:         sinfonia.CommitResponse{Ack: true},
			actionCommit: outcomeCommit,
		}
		if txn := n.inDoubt[entry.TransactionId]; txn != nil {
			n.releaseLocks(txn.heldLocks)
		}
		delete(n.inDoubt, entry.TransactionId)
		if err := storage.StoreProcessedPointer(n.storageDir, i+1); err != nil {
			n.mu.Unlock()
			return err
		}
		n.mu.Unlock()
		if flushNotify {
			go n.FlushAppliedNotifications(context.Background())
		}
	}
	n.mu.Lock()
	if n.restartRequired != nil {
		err := n.restartRequired
		n.mu.Unlock()
		return err
	}
	if err := n.decided.Retain(n.allLogTxIds); err != nil {
		err = n.decisionWriteErrorLocked(err)
		n.mu.Unlock()
		return err
	}
	if err := n.retainAppliedReceiptsLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.gcOutcomesLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	n.recoveryComplete = true
	n.mu.Unlock()
	return nil
}

func (n *Participant) callRecoverBoundaryHook(phase string, txId sinfonia.TransactionID) error {
	n.mu.Lock()
	hook := n.recoverBoundaryHook
	n.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(phase, txId)
}

func (n *Participant) callRecoverBoundaryHookLocked(phase string, txId sinfonia.TransactionID) error {
	if n.recoverBoundaryHook == nil {
		return nil
	}
	return n.recoverBoundaryHook(phase, txId)
}

func (n *Participant) HasForcedAbortForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.forcedAborts.IsForcedAbort(txId)
}

func (n *Participant) QueryInDoubt(ctx context.Context, _ sinfonia.QueryInDoubtRequest) (sinfonia.QueryInDoubtResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := sinfonia.QueryInDoubtResponse{Entries: make([]sinfonia.InDoubtEntry, 0, len(n.inDoubt))}
	for txId, tx := range n.inDoubt {
		resp.Entries = append(resp.Entries, sinfonia.InDoubtEntry{
			TID:          txId,
			PreparedAt:   tx.preparedAt.UnixNano(),
			Participants: append([]uint32(nil), tx.participants...),
		})
	}
	return resp, nil
}
