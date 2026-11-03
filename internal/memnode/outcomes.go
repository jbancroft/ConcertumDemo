package memnode

import (
	"errors"
	"maps"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrResponseUnavailable = errors.New("transaction exists without its original response")

type prepareOutcome struct {
	resp sinfonia.ExecPrepareResponse
}

type onePCOutcome struct {
	resp sinfonia.ExecCommit1PCResponse
}

// gcOutcomesLocked expires memory-only replies. In-flight transactions keep
// their replies; durable decisions are collected with their redo entries.
func (n *Participant) gcOutcomesLocked() error {
	retention := storage.TransactionRetention{
		CurrentEpoch: n.currentEpochLocked(), RetryHorizon: storage.TransactionEpochWindow,
		Keep: make(map[sinfonia.TransactionID]struct{}),
	}
	for tid := range n.inDoubt {
		retention.Keep[tid] = struct{}{}
	}
	if err := n.persistEpochFloorLocked(retention.CurrentEpoch); err != nil {
		return err
	}
	maps.DeleteFunc(n.prepareOutcomes, func(tid sinfonia.TransactionID, _ prepareOutcome) bool {
		return retention.Expired(tid)
	})
	maps.DeleteFunc(n.onePCOutcomes, func(tid sinfonia.TransactionID, _ onePCOutcome) bool {
		return retention.Expired(tid)
	})
	maps.DeleteFunc(n.commitOutcomes, func(tid sinfonia.TransactionID, _ commitOutcome) bool {
		return retention.Expired(tid)
	})
	return nil
}

func cloneReadData(in map[uint32][]byte) map[uint32][]byte {
	if in == nil {
		return nil
	}
	out := make(map[uint32][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func clonePrepareResponse(resp sinfonia.ExecPrepareResponse) sinfonia.ExecPrepareResponse {
	resp.ReadData = cloneReadData(resp.ReadData)
	if resp.LockConflict != nil {
		resp.LockConflict = new(*resp.LockConflict)
	}
	return resp
}

func clone1PCResponse(resp sinfonia.ExecCommit1PCResponse) sinfonia.ExecCommit1PCResponse {
	resp.ReadData = cloneReadData(resp.ReadData)
	if resp.LockConflict != nil {
		resp.LockConflict = new(*resp.LockConflict)
	}
	return resp
}

func (n *Participant) hasDurableTransactionLocked(txId sinfonia.TransactionID) bool {
	if _, ok := n.allLogTxIds[txId]; ok {
		return true
	}
	_, ok := n.decided.Get(txId)
	return ok
}

func (n *Participant) callResponseBoundaryHookLocked(phase string, txId sinfonia.TransactionID) error {
	if n.responseBoundaryHook == nil {
		return nil
	}
	return n.responseBoundaryHook(phase, txId)
}

func (n *Participant) rememberPrepareOutcomeLocked(txId sinfonia.TransactionID, resp sinfonia.ExecPrepareResponse) sinfonia.ExecPrepareResponse {
	n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(resp)}
	return resp
}

func (n *Participant) cachedPrepareOutcomeLocked(txId sinfonia.TransactionID, requestID sinfonia.RequestID, currentEpoch uint64) (sinfonia.ExecPrepareResponse, bool) {
	out, ok := n.prepareOutcomes[txId]
	if !ok {
		return sinfonia.ExecPrepareResponse{}, false
	}

	resp := clonePrepareResponse(out.resp)
	resp.RequestID = requestID
	resp.CurrentEpoch = currentEpoch
	return resp, true
}

func (n *Participant) rememberOnePCOutcomeLocked(txId sinfonia.TransactionID, resp sinfonia.ExecCommit1PCResponse) sinfonia.ExecCommit1PCResponse {
	n.onePCOutcomes[txId] = onePCOutcome{resp: clone1PCResponse(resp)}
	return resp
}

// rememberPendingResponseLocked keeps live reads available when recovery
// finishes an interrupted request. These responses do not survive a restart.
func (n *Participant) rememberPendingResponseLocked(txId sinfonia.TransactionID, txn *preparedTxn, commit bool) {
	if txn.prepareResponse != nil {
		n.rememberPrepareOutcomeLocked(txId, *txn.prepareResponse)
		txn.prepareResponse = nil
	}
	if commit && txn.onePCResponse != nil {
		n.rememberOnePCOutcomeLocked(txId, *txn.onePCResponse)
		txn.onePCResponse = nil
	}
}

func (n *Participant) SetResponseBoundaryHookForTest(fn func(phase string, txId sinfonia.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.responseBoundaryHook = fn
}
