package memnode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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
	n.mu.Lock()
	defer n.mu.Unlock()
	if out, ok := n.commitOutcomes[req.TID]; ok {
		if out.actionCommit {
			return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeCommit, VotedOK: true}, nil
		}
		return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeAbort, VotedOK: false}, nil
	}
	if n.decided != nil {
		if c, ok := n.decided.Get(req.TID); ok {
			if c {
				return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeCommit, VotedOK: true}, nil
			}
			return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeAbort, VotedOK: false}, nil
		}
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return sinfonia.QueryTidStatusResponse{KnownDecision: false, Outcome: sinfonia.TidOutcomeAbort, VotedOK: false}, nil
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		return sinfonia.QueryTidStatusResponse{KnownDecision: false, Outcome: sinfonia.TidOutcomeUnknown, VotedOK: true}, nil
	}
	if n.prepareVotedOKLocked(req.TID) {
		return sinfonia.QueryTidStatusResponse{KnownDecision: false, Outcome: sinfonia.TidOutcomeUnknown, VotedOK: true}, nil
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.AddItem(req.TID); err != nil {
			return sinfonia.QueryTidStatusResponse{}, err
		}
	}
	n.metrics.forcedAbortCount.Inc()
	n.metrics.forcedAbortCountCurrent.Set(int64(n.metrics.forcedAbortCountCurrent.Value() + 1))
	return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeAbort, VotedOK: false}, nil
}

func (n *Participant) RecoveryForceAbort(ctx context.Context, req sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	if !n.allowRate(n.recoveryLimiter, "recovery") {
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = n.correlationForTIDLocked(req.TID)
	}
	n.recordTimelineLocked(req.TID, correlationID, "recovery_force_abort", "")
	if out, ok := n.commitOutcomes[req.TID]; ok {
		if out.actionCommit {
			return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteOk, KnownDecision: true, DecisionCommit: true}, nil
		}
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadForced, KnownDecision: true}, nil
	}
	if n.decided != nil {
		if committed, ok := n.decided.Get(req.TID); ok {
			if committed {
				return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteOk, KnownDecision: true, DecisionCommit: true}, nil
			}
			return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadForced, KnownDecision: true}, nil
		}
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadForced}, nil
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteOk}, nil
	}
	if n.prepareVotedOKLocked(req.TID) {
		return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteOk}, nil
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.AddItem(req.TID); err != nil {
			return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadInternal}, err
		}
	}
	return sinfonia.RecoverForceAbortResponse{Vote: sinfonia.VoteBadForced}, nil
}

func (n *Participant) RecoveryDecide(ctx context.Context, req sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error) {
	if !n.allowRate(n.recoveryLimiter, "recovery") {
		return sinfonia.RecoverDecideResponse{Ack: false}, ErrResourceExhausted
	}
	resp, err := n.commit(ctx, sinfonia.CommitRequest{TID: req.TID, ActionCommit: req.ActionCommit, CorrelationID: req.CorrelationID}, true)
	if err != nil {
		return sinfonia.RecoverDecideResponse{Ack: false}, err
	}
	return sinfonia.RecoverDecideResponse{Ack: resp.Ack}, nil
}

// Recover resolves logged transactions using saved decisions and peer evidence.
// The node stays unavailable if it can't safely finish recovery.
func (n *Participant) Recover(ctx context.Context) error {
	n.metrics.recoveryRuns.Inc()
	n.mu.Lock()
	n.log(slog.LevelInfo, "Recovery initiated", "epoch", n.epoch)
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
		n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
		n.mu.Unlock()
		if flushNotify {
			go n.FlushAppliedNotifications(context.Background())
		}
	}
	n.mu.Lock()
	currentEpoch := n.currentEpochLocked()
	if err := n.persistEpochFloorLocked(currentEpoch); err != nil {
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()
	if n.replay != nil {
		if err := n.replay.Compact(storage.ReplayRetention{
			CurrentEpoch: currentEpoch,
			RetryHorizon: storage.PublicRetryHorizon,
		}); err != nil {
			n.mu.Lock()
			n.recoveryComplete = false
			n.mu.Unlock()
			return err
		}
	}
	n.mu.Lock()
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
