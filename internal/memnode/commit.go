package memnode

import (
	"context"
	"errors"
	"fmt"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

type onePCFlight struct {
	done chan struct{}
	resp sinfonia.ExecCommit1PCResponse
	err  error
}

type commitOutcome struct {
	resp         sinfonia.CommitResponse
	actionCommit bool
}

func (n *Participant) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return n.commit(ctx, req, false)
}

func (n *Participant) commit(ctx context.Context, req sinfonia.CommitRequest, recoveryDecision bool) (sinfonia.CommitResponse, error) {
	if !n.allowRate(n.commitLimiter, "commit") {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrResourceExhausted
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.CommitResponse{RequestID: req.RequestID}, n.restartRequired
	}
	if n.mode == ModeReplReplica {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
	}
	if recoveryDecision && ctx.Err() != nil {
		return sinfonia.CommitResponse{}, ctx.Err()
	}
	requestedAction := req.ActionCommit
	if out, ok := n.commitOutcomes[req.TID]; ok {
		resp := out.resp
		resp.RequestID = req.RequestID
		resp.Ack = !requestedAction || out.actionCommit
		return resp, nil
	}
	currentEpoch := n.currentEpochLocked()
	expired := (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID)
	if expired && n.inDoubt[req.TID] == nil && !n.hasDurableTransactionLocked(req.TID) {
		// Acknowledge harmless abort delivery without recreating forgotten evidence.
		if err := n.persistEpochFloorLocked(currentEpoch); err != nil {
			return sinfonia.CommitResponse{RequestID: req.RequestID}, err
		}
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: !req.ActionCommit}, nil
	}
	logged := false
	knownDecision := false
	if n.decided != nil {
		_, logged = n.allLogTxIds[req.TID]
		if durableAction, ok := n.decided.Get(req.TID); ok {
			// Keep the saved decision even if log cleanup or recovery removed
			// the other transaction state.
			req.ActionCommit = durableAction
			knownDecision = true
		}
	}
	if !recoveryDecision && !knownDecision && req.ActionCommit && n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, nil
	}
	if logged && !knownDecision {
		if err := n.decided.AddItem(req.TID, req.ActionCommit); err != nil {
			return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
		}
	}
	if n.mode == ModeReplPrimary && n.replica != nil {
		peer := n.replica
		n.mu.Unlock()
		replicationCtx := context.Background()
		if recoveryDecision {
			replicationCtx = ctx
		}
		replResp, replErr := peer.ReplicateCommitDecision(replicationCtx, sinfonia.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: req.ActionCommit})
		n.mu.Lock()
		if replErr != nil || !replResp.Ack {
			if replErr == nil {
				replErr = errors.New("replication commit decision not acknowledged")
			}
			n.setReplicationErrorLocked(replErr)
			return sinfonia.CommitResponse{RequestID: req.RequestID}, n.failUncertainLocked("replicate commit decision", replErr)
		}
		if n.restartRequired != nil {
			return sinfonia.CommitResponse{RequestID: req.RequestID}, n.restartRequired
		}
		n.setReplicationErrorLocked(nil)
	}
	if txn := n.inDoubt[req.TID]; txn != nil {
		if err := n.rememberPendingResponseLocked(ctx, req.TID, txn, req.ActionCommit); err != nil {
			return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
		}
	}
	resp, err := n.commitLocked(req)
	if err == nil && resp.Ack {
		n.commitOutcomes[req.TID] = commitOutcome{resp: resp, actionCommit: req.ActionCommit}
	}
	if err == nil && requestedAction && !req.ActionCommit {
		resp.Ack = false
	}
	return resp, err
}

func (n *Participant) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	acks := make([]bool, len(req.Decisions))
	for i, d := range req.Decisions {
		resp, err := n.Commit(ctx, d)
		if err != nil {
			return sinfonia.CommitBatchResponse{Acks: acks}, err
		}
		acks[i] = resp.Ack
	}
	return sinfonia.CommitBatchResponse{Acks: acks}, nil
}

// commitLocked saves decisions for logged transactions before applying writes,
// then releases the transaction's locks.
func (n *Participant) commitLocked(req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	if n.restartRequired != nil {
		return sinfonia.CommitResponse{RequestID: req.RequestID}, n.restartRequired
	}
	if n.decided != nil {
		if _, ok := n.allLogTxIds[req.TID]; ok {
			if durableAction, decided := n.decided.Get(req.TID); decided {
				req.ActionCommit = durableAction
			} else {
				if err := n.decided.AddItem(req.TID, req.ActionCommit); err != nil {
					return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
				}
			}
		}
	}

	txn, ok := n.inDoubt[req.TID]
	if !ok {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: true}, nil
	}

	flushNotify := false
	if req.ActionCommit {
		for _, item := range txn.writes {
			if err := n.write(item.Addr, item.Data); err != nil {
				n.recoveryComplete = false
				return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
			}
		}
		if txn.redoLoggedTid {
			n.appliedLocal[req.TID] = true
			n.enqueueAppliedNotificationsLocked(req.TID, txn.participants)
			flushNotify = true
		}
	}

	n.releaseLocks(txn.heldLocks)
	delete(n.inDoubt, req.TID)
	if flushNotify {
		go n.FlushAppliedNotifications(context.Background())
	}
	return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: true}, nil
}

func (n *Participant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (resp sinfonia.ExecCommit1PCResponse, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, n.restartRequired
	}
	if n.mode == ModeReplReplica {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if out, ok := n.onePCOutcomes[req.TID]; ok {
		original := clone1PCResponse(out.resp)
		if err := n.replicateReplayRecordLocked(ctx, req.TID, sinfonia.ReplayRecordOnePC, nil, &original); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		if err := n.finishPendingOnePCReplayLocked(req.TID); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		resp := original
		resp.RequestID = req.RequestID
		resp.CurrentEpoch = currentEpoch
		return resp, nil
	}
	if n.replay != nil {
		if original, ok := n.replay.GetOnePC(req.TID); ok {
			n.onePCOutcomes[req.TID] = onePCOutcome{resp: clone1PCResponse(original)}
			if err := n.replicateReplayRecordLocked(ctx, req.TID, sinfonia.ReplayRecordOnePC, nil, &original); err != nil {
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
			}
			if err := n.finishPendingOnePCReplayLocked(req.TID); err != nil {
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
			}
			original.RequestID = req.RequestID
			original.CurrentEpoch = currentEpoch
			return original, nil
		}
	}
	if flight, ok := n.onePCFlights[req.TID]; ok {
		n.mu.Unlock()
		select {
		case <-flight.done:
			n.mu.Lock()
			resp := clone1PCResponse(flight.resp)
			resp.RequestID = req.RequestID
			resp.CurrentEpoch = currentEpoch
			return resp, flight.err
		case <-ctx.Done():
			n.mu.Lock()
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, ctx.Err()
		}
	}
	flight := &onePCFlight{done: make(chan struct{})}
	n.onePCFlights[req.TID] = flight
	defer func() {
		flight.resp = clone1PCResponse(resp)
		flight.err = err
		delete(n.onePCFlights, req.TID)
		close(flight.done)
	}()

	if out, decided := n.commitOutcomes[req.TID]; n.hasDurableTransactionLocked(req.TID) || n.inDoubt[req.TID] != nil || (decided && out.actionCommit) {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: one-phase transaction %v", ErrReplayUnavailable, req.TID)
	}
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		if err := n.persistEpochFloorLocked(currentEpoch); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadStaleEpoch, CurrentEpoch: currentEpoch}, nil
	}
	if out, ok := n.commitOutcomes[req.TID]; ok && !out.actionCommit {
		return n.rememberOnePCOutcomeLocked(ctx, req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberOnePCOutcomeLocked(ctx, req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}

	if err := n.validateWriteRangesLocked(req.WriteItems); err != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	acquired, conflict := n.tryTransactionLocksLocked(req.CmpItems, req.ReadItems, req.WriteItems)
	if conflict != nil {
		return n.rememberOnePCOutcomeLocked(ctx, req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
	}

	for _, item := range req.CmpItems {
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			return n.rememberOnePCOutcomeLocked(ctx, req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
	}

	readData := make(map[uint32][]byte, len(req.ReadItems))
	for idx, item := range req.ReadItems {
		data, err := n.read(item.Addr, item.Len)
		if err != nil {
			n.releaseLocks(acquired)
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		readData[uint32(idx)] = data
	}

	// Retain the operation before an append or replica acknowledgement can
	// become uncertain. Recovery probes must not use it while the node is faulted.
	original := sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	txn := &preparedTxn{
		writes: cloneWrites(req.WriteItems), readData: cloneReadData(readData), heldLocks: acquired,
		preparedAt: n.nowFn(), participants: []uint32{n.hostId},
		onePCResponse: &original,
	}
	n.inDoubt[req.TID] = txn
	if n.redoLog != nil && len(req.WriteItems) > 0 {
		writes := cloneWrites(req.WriteItems)
		entry := storage.RedoLogItem{TransactionId: req.TID, HostIds: []uint32{n.hostId}, WriteItems: writes}
		if err := n.redoLog.AddItem(entry); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.failUncertainLocked("append one-phase redo", err)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = []uint32{n.hostId}
		txn.redoLoggedTid = true
		if n.mode == ModeReplPrimary && n.replica != nil {
			peer := n.replica
			n.mu.Unlock()
			replResp, replErr := peer.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{
				Entry: sinfonia.RedoEntryPayload{TID: req.TID, Participants: append([]uint32(nil), entry.HostIds...), WriteItems: writes},
			})
			n.mu.Lock()
			if replErr != nil || !replResp.Ack {
				if replErr == nil {
					replErr = errors.New("replication redo entry not acknowledged")
				}
				n.setReplicationErrorLocked(replErr)
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.failUncertainLocked("replicate one-phase redo", replErr)
			}
			if n.restartRequired != nil {
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.restartRequired
			}
			n.setReplicationErrorLocked(nil)
			n.mu.Unlock()
			replCommitResp, replCommitErr := peer.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: true})
			n.mu.Lock()
			if replCommitErr != nil || !replCommitResp.Ack {
				if replCommitErr == nil {
					replCommitErr = errors.New("replication commit decision not acknowledged")
				}
				n.setReplicationErrorLocked(replCommitErr)
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.failUncertainLocked("replicate one-phase decision", replCommitErr)
			}
			if n.restartRequired != nil {
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.restartRequired
			}
			n.setReplicationErrorLocked(nil)
		}
	}
	if n.decided != nil {
		if _, known := n.decided.Get(req.TID); !known {
			if err := n.decided.AddItem(req.TID, true); err != nil {
				n.recoveryComplete = false
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
			}
		}
	}
	if err := n.callOutcomeReplayBoundaryHookLocked("one-pc-before-response", req.TID); err != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	resp = clone1PCResponse(original)
	for _, item := range req.WriteItems {
		if err := n.write(item.Addr, item.Data); err != nil {
			n.recoveryComplete = false
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
	}
	var replayErr error
	resp, replayErr = n.rememberOnePCOutcomeLocked(ctx, req.TID, resp)
	if replayErr != nil {
		return resp, replayErr
	}
	// A duplicate may have finished this transaction while reply replication
	// released the mutex. Do not release a newer transaction's matching locks.
	if txn := n.inDoubt[req.TID]; txn != nil {
		n.releaseLocks(txn.heldLocks)
		delete(n.inDoubt, req.TID)
	}
	n.commitOutcomes[req.TID] = commitOutcome{resp: sinfonia.CommitResponse{RequestID: req.RequestID, Ack: true}, actionCommit: true}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		n.appliedLocal[req.TID] = true
		n.enqueueAppliedNotificationsLocked(req.TID, []uint32{n.hostId})
	}
	return resp, nil
}

// A retry can find a saved reply after its first delivery failed in replication.
// Finish the retained work as well, so a successful retry does not leave locks.
func (n *Participant) finishPendingOnePCReplayLocked(txId sinfonia.TransactionID) error {
	if txn := n.inDoubt[txId]; txn == nil || txn.onePCResponse == nil {
		return nil
	}
	resp, err := n.commitLocked(sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	if err == nil && resp.Ack {
		n.commitOutcomes[txId] = commitOutcome{resp: resp, actionCommit: true}
	}
	return err
}

func cloneWrites(items []sinfonia.WriteItem) []sinfonia.WriteItem {
	out := make([]sinfonia.WriteItem, len(items))
	for i, item := range items {
		out[i] = sinfonia.WriteItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...)}
	}
	return out
}

func (n *Participant) validateWriteRangesLocked(items []sinfonia.WriteItem) error {
	for _, item := range items {
		end := item.Addr + uint64(len(item.Data))
		if end < item.Addr || end > uint64(len(n.memory)) {
			return ErrOutOfBounds
		}
	}
	return nil
}
