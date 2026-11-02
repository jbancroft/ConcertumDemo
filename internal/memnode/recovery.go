package memnode

import (
	"context"
	"errors"
	"fmt"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrRecoveryPending = errors.New("participant recovery pending peer evidence")

type RecoveryPeer interface {
	QueryTidStatus(ctx context.Context, req sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error)
	NotifyApplied(ctx context.Context, req sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error)
}

func (n *Participant) currentEpochLocked() uint64 {
	current := sinfonia.EpochAt(n.nowFn())
	if current > n.epoch {
		n.epoch = current
	}
	return n.epoch
}

func (n *Participant) persistEpochFloorLocked(epoch uint64) error {
	if epoch > n.epoch {
		n.epoch = epoch
	}
	if epoch <= n.epochFloor {
		return nil
	}
	if n.storageDir != "" {
		if err := storage.StoreEpochFloor(n.storageDir, epoch); err != nil {
			return err
		}
	}
	n.epochFloor = epoch
	return nil
}

func (n *Participant) SetRecoveryPeers(peers map[uint32]RecoveryPeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.participants = make(map[uint32]RecoveryPeer, len(peers))
	for k, v := range peers {
		n.participants[k] = v
	}
}

func (n *Participant) QueryTidStatus(ctx context.Context, req sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error) {
	vote, err := n.recoveryVote(ctx, req.TID)
	if err != nil {
		return sinfonia.QueryTidStatusResponse{}, err
	}
	if vote.Expired {
		return sinfonia.QueryTidStatusResponse{}, fmt.Errorf("%w: transaction evidence expired", ErrRecoveryPending)
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
		out.Vote, out.ReadData = one.resp.Vote, cloneReadData(one.resp.ReadData)
		out.KnownDecision, out.DecisionCommit = true, one.resp.Vote == sinfonia.VoteOk
	} else if prepOK {
		out.Vote, out.ReadData = prep.resp.Vote, cloneReadData(prep.resp.ReadData)
	}
	if tx := n.inDoubt[tid]; tx != nil {
		out.Vote = sinfonia.VoteOk
		if out.ReadData == nil {
			out.ReadData = cloneReadData(tx.readData)
		}
	}
	if decision, ok := n.commitOutcomes[tid]; ok {
		out.KnownDecision, out.DecisionCommit = true, decision.actionCommit
	} else if n.decided != nil {
		if commit, ok := n.decided.Get(tid); ok {
			out.KnownDecision, out.DecisionCommit = true, commit
		}
	}
	if out.KnownDecision {
		out.Vote = sinfonia.VoteBadForced
		if out.DecisionCommit {
			out.Vote = sinfonia.VoteOk
		}
		return out, nil
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(tid) {
		out.Vote = sinfonia.VoteBadForced
		return out, nil
	}
	if _, ok := n.allLogTxIds[tid]; ok {
		out.Vote = sinfonia.VoteOk
	}
	if out.Vote != sinfonia.VoteUnknown {
		return out, nil
	}
	// Missing evidence after retention is not evidence of an abort.
	if (storage.TransactionRetention{CurrentEpoch: n.currentEpochLocked(), RetryHorizon: storage.PublicRetryHorizon}).Expired(tid) {
		out.Expired = true
		return out, nil
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.AddItem(tid); err != nil {
			return out, n.failUncertainLocked("append forced abort", err)
		}
	} else {
		// RAM nodes also have to reject a late request for this same attempt.
		n.commitOutcomes[tid] = commitOutcome{actionCommit: false}
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
	entries := []storage.RedoLogItem{}
	if n.redoLog != nil {
		entries = n.redoLog.Entries()
	}
	ptr := 0
	var err error
	if n.storageDir != "" {
		ptr, err = storage.LoadProcessedPointer(n.storageDir)
		if err != nil {
			n.mu.Unlock()
			return err
		}
	}
	if ptr < 0 {
		ptr = 0
	}
	peers := make(map[uint32]RecoveryPeer, len(n.participants))
	for k, v := range n.participants {
		peers[k] = v
	}
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
		if n.decided != nil {
			if c, ok := n.decided.Get(entry.TransactionId); ok {
				outcomeCommit = c
				known = true
			}
		}
		n.mu.Unlock()

		if !known {
			outcomeCommit, err = n.discoverOutcome(ctx, peers, entry)
			if err != nil {
				return err
			}
		}

		n.mu.Lock()
		if n.restartRequired != nil {
			err := n.restartRequired
			n.mu.Unlock()
			return err
		}
		if n.decided != nil {
			if decidedCommit, decided := n.decided.Get(entry.TransactionId); decided {
				outcomeCommit = decidedCommit
			} else if err := n.decided.AddItem(entry.TransactionId, outcomeCommit); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		flushNotify := false
		if outcomeCommit {
			if err := n.callRecoverBoundaryHookLocked("apply-before", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
			for _, w := range entry.WriteItems {
				if err := n.write(w.Addr, w.Data); err != nil {
					n.mu.Unlock()
					return err
				}
			}
			n.appliedLocal[entry.TransactionId] = true
			n.enqueueAppliedNotificationsLocked(entry.TransactionId, entry.HostIds)
			flushNotify = true
			if err := n.callRecoverBoundaryHookLocked("apply-after", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		if n.storageDir != "" {
			if err := n.callRecoverBoundaryHookLocked("pointer-before", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
			if err := storage.StoreProcessedPointer(n.storageDir, i+1); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		n.commitOutcomes[entry.TransactionId] = commitOutcome{
			resp:         sinfonia.CommitResponse{Ack: true},
			actionCommit: outcomeCommit,
		}
		delete(n.inDoubt, entry.TransactionId)
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

func (n *Participant) discoverOutcome(ctx context.Context, peers map[uint32]RecoveryPeer, entry storage.RedoLogItem) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	n.mu.Lock()
	peerTimeout := n.peerTimeout
	forcedAbort := n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(entry.TransactionId)
	n.mu.Unlock()

	knownDecision := false
	knownCommit := false
	negativeVote := forcedAbort
	for _, pid := range entry.HostIds {
		if pid == n.hostId {
			continue
		}
		if peers[pid] == nil {
			return false, fmt.Errorf("%w: transaction %v participant %d is not configured", ErrRecoveryPending, entry.TransactionId, pid)
		}
	}
	for _, pid := range entry.HostIds {
		if pid == n.hostId {
			continue
		}
		peer := peers[pid]
		queryCtx, cancel := context.WithTimeout(ctx, peerTimeout)
		resp, err := peer.QueryTidStatus(queryCtx, sinfonia.QueryTidStatusRequest{TID: entry.TransactionId})
		cancel()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, fmt.Errorf("%w: transaction %v participant %d: %v", ErrRecoveryPending, entry.TransactionId, pid, err)
		}
		if resp.KnownDecision {
			switch resp.Outcome {
			case sinfonia.TidOutcomeCommit:
				if knownDecision && !knownCommit {
					return false, fmt.Errorf("%w: transaction %v has conflicting durable decisions", ErrRecoveryPending, entry.TransactionId)
				}
				knownDecision = true
				knownCommit = true
			case sinfonia.TidOutcomeAbort:
				if knownDecision && knownCommit {
					return false, fmt.Errorf("%w: transaction %v has conflicting durable decisions", ErrRecoveryPending, entry.TransactionId)
				}
				knownDecision = true
				knownCommit = false
			default:
				return false, fmt.Errorf("%w: transaction %v participant %d reported an unknown decision", ErrRecoveryPending, entry.TransactionId, pid)
			}
			continue
		}
		if !resp.VotedOK || resp.Outcome == sinfonia.TidOutcomeAbort {
			negativeVote = true
		}
	}
	if knownDecision {
		return knownCommit, nil
	}
	if negativeVote {
		return false, nil
	}
	return true, nil
}

func (n *Participant) SetRecoverBoundaryHookForTest(fn func(phase string, txId sinfonia.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recoverBoundaryHook = fn
}

func (n *Participant) HasDecidedForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.decided == nil {
		return false
	}
	_, ok := n.decided.Get(txId)
	return ok
}

func (n *Participant) HasForcedAbortForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return false
	}
	return n.forcedAborts.IsForcedAbort(txId)
}

func (n *Participant) ForceAbortForTest(txId sinfonia.TransactionID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return errors.New("forced-abort store unavailable")
	}
	return n.forcedAborts.AddItem(txId)
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
