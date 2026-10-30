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

type prepareFlight struct {
	done chan struct{}
	resp sinfonia.ExecPrepareResponse
	err  error
}

// preparedTxn keeps the writes and locks until a commit or abort decision arrives.
// Durable nodes also log prepared writes so they can recover after a restart.
type preparedTxn struct {
	writes        []sinfonia.WriteItem
	readData      map[uint32][]byte
	heldLocks     []RangeLock
	preparedAt    time.Time
	participants  []uint32
	redoLoggedTid bool
	class         string
	correlationID string
}

func (n *Participant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (resp sinfonia.ExecPrepareResponse, err error) {
	if !n.allowRate(n.prepareLimiter, "prepare") {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	select {
	case n.prepareSlots <- struct{}{}:
		defer func() { <-n.prepareSlots }()
	default:
		n.metrics.rejectedRequestsTotal.Inc()
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	if n.maxItemsPerTxn > 0 && len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems) > n.maxItemsPerTxn {
		n.metrics.rejectedRequestsTotal.Inc()
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	var reqBytes int64
	for _, i := range req.ReadItems {
		reqBytes += int64(i.Len)
	}
	for _, i := range req.WriteItems {
		reqBytes += int64(len(i.Data))
	}
	if (n.maxRequestBytes > 0 && reqBytes > int64(n.maxRequestBytes)) || ((n.maxReadBytesPerTxn > 0 || n.maxWriteBytesPerTxn > 0) && reqBytes > n.maxReadBytesPerTxn+n.maxWriteBytesPerTxn) {
		n.metrics.requestSizeRejectedTotal.Inc()
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}

	start := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if resp, ok, replayErr := n.replayPrepareOutcomeLocked(req.TID, req.RequestID, currentEpoch); ok || replayErr != nil {
		if replayErr != nil {
			return resp, replayErr
		}
		return resp, nil
	}
	if flight, ok := n.prepareFlights[req.TID]; ok {
		n.mu.Unlock()
		select {
		case <-flight.done:
			n.mu.Lock()
			resp := clonePrepareResponse(flight.resp)
			resp.RequestID = req.RequestID
			resp.CurrentEpoch = currentEpoch
			return resp, flight.err
		case <-ctx.Done():
			n.mu.Lock()
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, ctx.Err()
		}
	}
	flight := &prepareFlight{done: make(chan struct{})}
	n.prepareFlights[req.TID] = flight
	defer func() {
		flight.resp = clonePrepareResponse(resp)
		flight.err = err
		delete(n.prepareFlights, req.TID)
		close(flight.done)
	}()

	if n.hasDurableTransactionLocked(req.TID) {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: prepare transaction %v", ErrReplayUnavailable, req.TID)
	}
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadStaleEpoch, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if out, ok := n.commitOutcomes[req.TID]; ok && !out.actionCommit {
		return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if txn, ok := n.inDoubt[req.TID]; ok {
		resp := sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteOk, ReadData: cloneReadData(txn.readData), CurrentEpoch: currentEpoch}
		return resp, nil
	}

	if err := n.validateWriteRangesLocked(req.WriteItems); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	if n.maxInDoubtHard > 0 && len(n.inDoubt) >= n.maxInDoubtHard {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, ErrResourceExhausted
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.TID.CorrelationID()
	}
	n.inDoubt[req.TID] = &preparedTxn{preparedAt: n.nowFn(), participants: append([]uint32(nil), req.Participants...), class: req.Class, correlationID: correlationID}
	txn := n.inDoubt[req.TID]
	n.recordTimelineLocked(req.TID, correlationID, "prepare_received", "")
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
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "write-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.CmpItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "cmp-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.ReadItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "read-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}

	for _, item := range req.CmpItems {
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
	}

	readData := make(map[uint32][]byte, len(req.ReadItems))
	for idx, item := range req.ReadItems {
		data, err := n.read(item.Addr, item.Len)
		if err != nil {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		readData[uint32(idx)] = data
	}

	// Save our yes vote before replying. Even with no local writes, we need an
	// empty entry if the transaction writes elsewhere: recovery needs every vote.
	if n.redoLog != nil && (req.HasWrites || len(req.WriteItems) > 0) {
		participants := append([]uint32(nil), req.Participants...)
		clonedWrites := cloneWrites(req.WriteItems)
		if err := n.redoLog.AddItem(storage.RedoLogItem{TransactionId: req.TID, HostIds: participants, WriteItems: clonedWrites}); err != nil {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		if n.mode == ModeReplPrimary && n.replica != nil {
			n.mu.Unlock()
			replStart := time.Now()
			replResp, replErr := n.replica.ReplicateRedoEntry(context.Background(), sinfonia.ReplicateRedoEntryRequest{Entry: sinfonia.RedoEntryPayload{TID: req.TID, Participants: participants, WriteItems: clonedWrites}})
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
				delete(n.inDoubt, req.TID)
				return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = participants
		txn.redoLoggedTid = true
	}

	txn.writes = cloneWrites(req.WriteItems)
	txn.readData = readData
	txn.heldLocks = acquired

	n.metrics.prepareLatency.Observe(time.Since(start))
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
	n.log(slog.LevelInfo, "ExecAndPrepare received", "tid", req.TID, "epoch", req.TID.Epoch)
	n.log(slog.LevelInfo, "Vote decision", "tid", req.TID, "epoch", req.TID.Epoch, "vote", sinfonia.VoteOk)
	n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_ok", "")
	resp = sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	if err := n.callOutcomeReplayBoundaryHookLocked("prepare-before-response", req.TID); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	return n.rememberPrepareOutcomeLocked(req.TID, resp)
}
