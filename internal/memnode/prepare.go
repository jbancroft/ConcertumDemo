package memnode

import (
	"context"
	"fmt"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

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

func (n *Participant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal}, n.restartRequired
	}

	currentEpoch := n.currentEpochLocked()
	if resp, ok := n.replayPrepareOutcomeLocked(req.TID, req.RequestID, currentEpoch); ok {
		return resp, nil
	}

	if n.hasDurableTransactionLocked(req.TID) {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: prepare transaction %v", ErrReplayUnavailable, req.TID)
	}
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		if err := n.persistEpochFloorLocked(currentEpoch); err != nil {
			return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadStaleEpoch, CurrentEpoch: currentEpoch}, nil
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
	n.inDoubt[req.TID] = &preparedTxn{preparedAt: n.nowFn(), participants: append([]uint32(nil), req.Participants...)}
	txn := n.inDoubt[req.TID]
	acquired, conflict := n.tryTransactionLocksLocked(req.CmpItems, req.ReadItems, req.WriteItems)
	if conflict != nil {
		delete(n.inDoubt, req.TID)
		return n.rememberPrepareOutcomeLocked(req.TID, sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
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
		txn.redoLoggedTid = true
	}
	resp := clonePrepareResponse(original)
	if err := n.callOutcomeReplayBoundaryHookLocked("prepare-before-response", req.TID); err != nil {
		return sinfonia.ExecPrepareResponse{RequestID: req.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	resp, err := n.rememberPrepareOutcomeLocked(req.TID, resp)
	if err == nil {
		txn.prepareResponse = nil
	}
	return resp, err
}
