package memnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
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
	// Only live requests have exact responses; redo alone cannot reconstruct reads.
	prepareResponse *sinfonia.ExecPrepareResponse
	onePCResponse   *sinfonia.ExecCommit1PCResponse
}

func (n *Participant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (resp sinfonia.ExecPrepareResponse, err error) {
	if !n.allowRate(n.prepareLimiter, "prepare") {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	select {
	case n.prepareSlots <- struct{}{}:
		defer func() { <-n.prepareSlots }()
	default:
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	if n.maxItemsPerTxn > 0 && len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems) > n.maxItemsPerTxn {
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
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrResourceExhausted
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, n.restartRequired
	}
	if n.mode == ModeReplReplica {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if resp, ok, replayErr := n.replayPrepareOutcomeLocked(ctx, req.TID, req.RequestID, currentEpoch); ok || replayErr != nil {
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
		return n.rememberPrepareOutcomeLocked(ctx, req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadStaleEpoch, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberPrepareOutcomeLocked(ctx, req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if out, ok := n.commitOutcomes[req.TID]; ok && !out.actionCommit {
		return n.rememberPrepareOutcomeLocked(ctx, req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadForced, CurrentEpoch: currentEpoch})
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
	n.inDoubt[req.TID] = &preparedTxn{preparedAt: n.nowFn(), participants: append([]uint32(nil), req.Participants...)}
	txn := n.inDoubt[req.TID]
	acquired, conflict := n.tryTransactionLocksLocked(req.CmpItems, req.ReadItems, req.WriteItems)
	if conflict != nil {
		delete(n.inDoubt, req.TID)
		return n.rememberPrepareOutcomeLocked(ctx, req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
	}

	for _, item := range req.CmpItems {
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return n.rememberPrepareOutcomeLocked(ctx, req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadCmp, CurrentEpoch: currentEpoch})
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

	// Retain the complete operation before persistence can become uncertain.
	txn.writes = cloneWrites(req.WriteItems)
	txn.readData = readData
	txn.heldLocks = acquired
	original := sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	txn.prepareResponse = &original

	// Save our yes vote before replying. Even with no local writes, we need an
	// empty entry if the transaction writes elsewhere: recovery needs every vote.
	if n.redoLog != nil && (req.HasWrites || len(req.WriteItems) > 0) {
		participants := append([]uint32(nil), req.Participants...)
		clonedWrites := cloneWrites(req.WriteItems)
		if err := n.redoLog.AddItem(storage.RedoLogItem{TransactionId: req.TID, HostIds: participants, WriteItems: clonedWrites}); err != nil {
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.failUncertainLocked("append prepare redo", err)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = participants
		txn.redoLoggedTid = true
		if n.mode == ModeReplPrimary && n.replica != nil {
			peer := n.replica
			n.mu.Unlock()
			replResp, replErr := peer.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{Entry: sinfonia.RedoEntryPayload{TID: req.TID, Participants: participants, WriteItems: clonedWrites}})
			n.mu.Lock()
			if replErr != nil || !replResp.Ack {
				if replErr == nil {
					replErr = errors.New("replication redo entry not acknowledged")
				}
				n.setReplicationErrorLocked(replErr)
				return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.failUncertainLocked("replicate prepare redo", replErr)
			}
			if n.restartRequired != nil {
				return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, n.restartRequired
			}
			n.setReplicationErrorLocked(nil)
		}
	}
	resp = clonePrepareResponse(original)
	if err := n.callOutcomeReplayBoundaryHookLocked("prepare-before-response", req.TID); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	resp, err = n.rememberPrepareOutcomeLocked(ctx, req.TID, resp)
	if err == nil {
		txn.prepareResponse = nil
	}
	return resp, err
}
