package memnode

import (
	"errors"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrReplayUnavailable = errors.New("durable transaction exists without exact replay payload")

type prepareOutcome struct {
	resp sinfonia.ExecPrepareResponse
}

type onePCOutcome struct {
	resp sinfonia.ExecCommit1PCResponse
}

// gcReplayLocked expires the same history in memory and on disk.
// Saving replies holds the same mutex, so it cannot race compaction.
func (n *Participant) gcReplayLocked() error {
	retention := storage.ReplayRetention{
		CurrentEpoch: n.currentEpochLocked(), RetryHorizon: storage.PublicRetryHorizon,
		Keep: make(map[sinfonia.TransactionID]struct{}),
	}
	for tid := range n.inDoubt {
		retention.Keep[tid] = struct{}{}
	}
	if err := n.persistEpochFloorLocked(retention.CurrentEpoch); err != nil {
		return err
	}
	if n.replay != nil {
		if err := n.replay.Compact(retention); err != nil {
			return err
		}
	}
	if n.decided != nil {
		// Redo recovery still needs these decisions after exact replies expire.
		decisions := retention
		decisions.Keep = make(map[sinfonia.TransactionID]struct{})
		for tid := range retention.Keep {
			decisions.Keep[tid] = struct{}{}
		}
		for tid := range n.allLogTxIds {
			decisions.Keep[tid] = struct{}{}
		}
		if err := n.decided.Compact(decisions); err != nil {
			return err
		}
	}
	for tid := range n.prepareOutcomes {
		if retention.Expired(tid) {
			delete(n.prepareOutcomes, tid)
		}
	}
	for tid := range n.onePCOutcomes {
		if retention.Expired(tid) {
			delete(n.onePCOutcomes, tid)
		}
	}
	for tid := range n.commitOutcomes {
		if retention.Expired(tid) {
			delete(n.commitOutcomes, tid)
		}
	}
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
		conflict := *resp.LockConflict
		resp.LockConflict = &conflict
	}
	return resp
}

func clone1PCResponse(resp sinfonia.ExecCommit1PCResponse) sinfonia.ExecCommit1PCResponse {
	resp.ReadData = cloneReadData(resp.ReadData)
	if resp.LockConflict != nil {
		conflict := *resp.LockConflict
		resp.LockConflict = &conflict
	}
	return resp
}

func (n *Participant) hasDurableTransactionLocked(txId sinfonia.TransactionID) bool {
	if _, ok := n.allLogTxIds[txId]; ok {
		return true
	}
	if n.decided != nil {
		_, ok := n.decided.Get(txId)
		return ok
	}
	return false
}

func (n *Participant) callOutcomeReplayBoundaryHookLocked(phase string, txId sinfonia.TransactionID) error {
	if n.outcomeReplayBoundaryHook == nil {
		return nil
	}
	return n.outcomeReplayBoundaryHook(phase, txId)
}

func (n *Participant) rememberPrepareOutcomeLocked(txId sinfonia.TransactionID, resp sinfonia.ExecPrepareResponse) (sinfonia.ExecPrepareResponse, error) {
	if n.replay != nil {
		if err := n.replay.AppendPrepare(txId, resp); err != nil {
			return sinfonia.ExecPrepareResponse{RequestID: resp.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
		}
	}

	n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(resp)}
	return resp, nil
}

func (n *Participant) replayPrepareOutcomeLocked(txId sinfonia.TransactionID, requestID sinfonia.RequestID, currentEpoch uint64) (sinfonia.ExecPrepareResponse, bool) {
	var original sinfonia.ExecPrepareResponse
	if out, ok := n.prepareOutcomes[txId]; ok {
		original = clonePrepareResponse(out.resp)
	} else {
		if n.replay == nil {
			return sinfonia.ExecPrepareResponse{}, false
		}
		var ok bool
		original, ok = n.replay.GetPrepare(txId)
		if !ok {
			return sinfonia.ExecPrepareResponse{}, false
		}
		n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(original)}
	}

	resp := original
	resp.RequestID = requestID
	resp.CurrentEpoch = currentEpoch
	return resp, true
}

func (n *Participant) rememberOnePCOutcomeLocked(txId sinfonia.TransactionID, resp sinfonia.ExecCommit1PCResponse) (sinfonia.ExecCommit1PCResponse, error) {
	if n.replay != nil {
		if err := n.replay.AppendOnePC(txId, resp); err != nil {
			return sinfonia.ExecCommit1PCResponse{RequestID: resp.RequestID, Vote: sinfonia.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
		}
	}

	n.onePCOutcomes[txId] = onePCOutcome{resp: clone1PCResponse(resp)}
	return resp, nil
}

// rememberPendingResponseLocked preserves the original reply before recovery
// discards its only remaining copy. The existing replay store rejects overwrites.
func (n *Participant) rememberPendingResponseLocked(txId sinfonia.TransactionID, txn *preparedTxn, commit bool) error {
	if txn.prepareResponse != nil {
		if _, err := n.rememberPrepareOutcomeLocked(txId, *txn.prepareResponse); err != nil {
			return err
		}
		txn.prepareResponse = nil
	}
	if commit && txn.onePCResponse != nil {
		if _, err := n.rememberOnePCOutcomeLocked(txId, *txn.onePCResponse); err != nil {
			return err
		}
		txn.onePCResponse = nil
	}
	return nil
}

func (n *Participant) SetOutcomeReplayBoundaryHookForTest(fn func(phase string, txId sinfonia.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.outcomeReplayBoundaryHook = fn
}
