package memnode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
	"github.com/jbancroft/ConcertumDemo/minitxn"
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

	start := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, err
	}
	requestedAction := req.ActionCommit
	if out, ok := n.commitOutcomes[req.TID]; ok {
		resp := out.resp
		resp.RequestID = req.RequestID
		resp.Ack = !requestedAction || out.actionCommit
		return resp, nil
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
		n.mu.Unlock()
		replResp, replErr := n.replica.ReplicateCommitDecision(context.Background(), sinfonia.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: req.ActionCommit})
		n.mu.Lock()
		if replErr != nil || !replResp.Ack {
			n.metrics.replicationFailures.Inc()
			if replErr != nil {
				n.setReplicationErrorLocked(replErr)
			} else {
				n.setReplicationErrorLocked(errors.New("replication commit decision not acknowledged"))
			}
			return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, errors.New("replication commit decision failed")
		}
		n.setReplicationErrorLocked(nil)
	}
	class := n.classForTIDLocked(req.TID)
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = n.correlationForTIDLocked(req.TID)
	}
	n.recordTimelineLocked(req.TID, correlationID, "commit_decision_received", "")
	resp, err := n.commitLocked(req)
	if err == nil && resp.Ack {
		n.commitOutcomes[req.TID] = commitOutcome{resp: resp, actionCommit: req.ActionCommit}
	}
	n.metrics.commitLatency.Observe(time.Since(start))
	n.metrics.endToEndTxnLatency.Observe(time.Since(start))
	n.metrics.minitransactionsTotal.Inc()
	if req.ActionCommit {
		n.metrics.minitransactionsCommitted.Inc()
	} else {
		n.metrics.minitransactionsAborted.Inc()
	}
	n.metrics.ObserveClassDecision(class, req.ActionCommit, n.nowFn())
	n.log(slog.LevelInfo, "Commit applied", "tid", req.TID, "epoch", req.TID.Epoch, "actionCommit", req.ActionCommit)
	if req.ActionCommit {
		n.recordTimelineLocked(req.TID, correlationID, "commit_applied", "2pc")
	} else {
		n.recordTimelineLocked(req.TID, correlationID, "abort_applied", "2pc")
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
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
	if flushNotify {
		go n.FlushAppliedNotifications(context.Background())
	}
	return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: true}, nil
}

func (n *Participant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (resp sinfonia.ExecCommit1PCResponse, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if out, ok := n.onePCOutcomes[req.TID]; ok {
		original := clone1PCResponse(out.resp)
		if err := n.replicateReplayRecordLocked(req.TID, sinfonia.ReplayRecordOnePC, nil, &original); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		resp := original
		resp.RequestID = req.RequestID
		resp.CurrentEpoch = currentEpoch
		return resp, nil
	}
	correlationID := req.CorrelationID
	if n.replay != nil {
		if original, ok := n.replay.GetOnePC(req.TID); ok {
			n.onePCOutcomes[req.TID] = onePCOutcome{resp: clone1PCResponse(original)}
			if err := n.replicateReplayRecordLocked(req.TID, sinfonia.ReplayRecordOnePC, nil, &original); err != nil {
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

	if n.hasDurableTransactionLocked(req.TID) {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: one-phase transaction %v", ErrReplayUnavailable, req.TID)
	}
	if correlationID == "" {
		correlationID = req.TID.CorrelationID()
	}
	n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_received", "")
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadStaleEpoch, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}

	if err := n.validateWriteRangesLocked(req.WriteItems); err != nil {
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	acquired := make([]RangeLock, 0, len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems))
	lockRead := func(item minitxn.RangeItem) (bool, *sinfonia.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: false}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, false)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}
	lockWrite := func(item minitxn.RangeItem) (bool, *sinfonia.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: true}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, true)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}

	for _, item := range req.WriteItems {
		if ok, conflict := lockWrite(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "write-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.CmpItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "cmp-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.ReadItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "read-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}

	for _, item := range req.CmpItems {
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			return n.rememberOnePCOutcomeLocked(req.TID, sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadCmp, CurrentEpoch: currentEpoch})
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

	if n.redoLog != nil && len(req.WriteItems) > 0 {
		writes := cloneWrites(req.WriteItems)
		entry := storage.RedoLogItem{TransactionId: req.TID, HostIds: []uint32{n.hostId}, WriteItems: writes}
		if err := n.redoLog.AddItem(entry); err != nil {
			n.releaseLocks(acquired)
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		if n.mode == ModeReplPrimary && n.replica != nil {
			n.mu.Unlock()
			replStart := time.Now()
			replResp, replErr := n.replica.ReplicateRedoEntry(context.Background(), sinfonia.ReplicateRedoEntryRequest{
				Entry: sinfonia.RedoEntryPayload{TID: req.TID, Participants: append([]uint32(nil), entry.HostIds...), WriteItems: writes},
			})
			n.metrics.replicationRoundtripLatency.Observe(time.Since(replStart))
			n.metrics.replicationEntriesSent.Inc()
			n.mu.Lock()
			if replErr != nil || !replResp.Ack {
				n.metrics.replicationFailures.Inc()
				if replErr != nil {
					n.setReplicationErrorLocked(replErr)
				} else {
					n.setReplicationErrorLocked(errors.New("replication redo entry not acknowledged"))
				}
				n.releaseLocks(acquired)
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
			n.mu.Unlock()
			replCommitResp, replCommitErr := n.replica.ReplicateCommitDecision(context.Background(), sinfonia.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: true})
			n.mu.Lock()
			if replCommitErr != nil || !replCommitResp.Ack {
				n.metrics.replicationFailures.Inc()
				if replCommitErr != nil {
					n.setReplicationErrorLocked(replCommitErr)
				} else {
					n.setReplicationErrorLocked(errors.New("replication commit decision not acknowledged"))
				}
				n.releaseLocks(acquired)
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = []uint32{n.hostId}
	}
	if n.decided != nil {
		if _, known := n.decided.Get(req.TID); !known {
			if err := n.decided.AddItem(req.TID, true); err != nil {
				n.releaseLocks(acquired)
				return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
		}
	}
	if err := n.callOutcomeReplayBoundaryHookLocked("one-pc-before-response", req.TID); err != nil {
		n.releaseLocks(acquired)
		return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	resp = sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	for _, item := range req.WriteItems {
		if err := n.write(item.Addr, item.Data); err != nil {
			n.recoveryComplete = false
			n.releaseLocks(acquired)
			return sinfonia.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
	}
	n.releaseLocks(acquired)
	var replayErr error
	resp, replayErr = n.rememberOnePCOutcomeLocked(req.TID, resp)
	if replayErr != nil {
		return resp, replayErr
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		n.appliedLocal[req.TID] = true
		n.enqueueAppliedNotificationsLocked(req.TID, []uint32{n.hostId})
	}
	n.metrics.minitransactionsTotal.Inc()
	n.metrics.minitransactionsCommitted.Inc()
	n.metrics.ObserveClassDecision(req.Class, true, n.nowFn())
	n.recordTimelineLocked(req.TID, correlationID, "commit_applied", "1pc")
	n.commitOutcomes[req.TID] = commitOutcome{resp: sinfonia.CommitResponse{RequestID: req.RequestID, Ack: true}, actionCommit: true}
	return resp, nil
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
