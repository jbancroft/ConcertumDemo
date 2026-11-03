package memnode

import (
	"context"
	"os"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func (n *Participant) NotifyApplied(ctx context.Context, req sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, txId := range req.TIDs {
		if _, ok := n.allLogTxIds[txId]; !ok {
			continue
		}
		if _, ok := n.appliedRemote[txId]; !ok {
			n.appliedRemote[txId] = map[uint32]bool{}
		}
		n.appliedRemote[txId][req.FromMemID] = true
	}
	return sinfonia.NotifyAppliedResponse{Ack: true}, nil
}

func (n *Participant) enqueueAppliedNotificationsLocked(txId sinfonia.TransactionID, participants []uint32) {
	for _, pid := range participants {
		if pid == n.hostId {
			continue
		}
		if _, ok := n.pendingNotify[pid]; !ok {
			n.pendingNotify[pid] = map[sinfonia.TransactionID]struct{}{}
		}
		n.pendingNotify[pid][txId] = struct{}{}
	}
}

func (n *Participant) FlushAppliedNotifications(ctx context.Context) {
	n.mu.Lock()
	peers := make(map[uint32]RecoveryPeer, len(n.participants))
	for pid, p := range n.participants {
		peers[pid] = p
	}
	pending := make(map[uint32][]sinfonia.TransactionID, len(n.pendingNotify))
	for pid, txIds := range n.pendingNotify {
		list := make([]sinfonia.TransactionID, 0, len(txIds))
		for txId := range txIds {
			list = append(list, txId)
		}
		pending[pid] = list
	}
	n.mu.Unlock()

	for pid, txIds := range pending {
		if len(txIds) == 0 {
			continue
		}
		peer := peers[pid]
		if peer == nil {
			continue
		}
		resp, err := peer.NotifyApplied(ctx, sinfonia.NotifyAppliedRequest{FromMemID: n.hostId, TIDs: txIds})
		if err != nil || !resp.Ack {
			continue
		}
		n.mu.Lock()
		if existing, ok := n.pendingNotify[pid]; ok {
			for _, txId := range txIds {
				delete(existing, txId)
			}
			if len(existing) == 0 {
				delete(n.pendingNotify, pid)
			}
		}
		n.mu.Unlock()
	}
}

func (n *Participant) RunRedoLogGC(ctx context.Context) error {
	n.FlushAppliedNotifications(ctx)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog == nil {
		return nil
	}
	// Startup recovery uses a snapshot of log indexes and writes its checkpoint
	// between entries. Do not compact that log until recovery has finished.
	if !n.recoveryComplete {
		return ErrRecoveryPending
	}
	entries := n.redoLog.Entries()
	truncateCount := 0
	for _, entry := range entries {
		committed, known := false, false
		if n.decided != nil {
			committed, known = n.decided.Get(entry.TransactionId)
		}
		if !known {
			break
		}
		if !committed {
			truncateCount++
			continue
		}
		if !n.appliedLocal[entry.TransactionId] {
			break
		}
		allApplied := true
		for _, pid := range entry.HostIds {
			if pid == n.hostId {
				continue
			}
			if !n.appliedRemote[entry.TransactionId][pid] {
				allApplied = false
				break
			}
		}
		if !allApplied {
			break
		}
		truncateCount++
	}
	removed := entries[:truncateCount]
	if truncateCount > 0 {
		if err := n.redoLog.TruncateHead(truncateCount); err != nil {
			n.recoveryComplete = false
			return err
		}
	}
	for _, entry := range removed {
		delete(n.allLogTxIds, entry.TransactionId)
		delete(n.inDoubt, entry.TransactionId)
		delete(n.prepareOutcomes, entry.TransactionId)
		delete(n.onePCOutcomes, entry.TransactionId)
		delete(n.commitOutcomes, entry.TransactionId)
		delete(n.appliedLocal, entry.TransactionId)
		delete(n.appliedRemote, entry.TransactionId)
		for pid, txIds := range n.pendingNotify {
			delete(txIds, entry.TransactionId)
			if len(txIds) == 0 {
				delete(n.pendingNotify, pid)
			}
		}
	}
	// Redo must disappear durably before its decision. A crash between these
	// steps leaves harmless extra decisions that startup recovery removes.
	if err := n.decided.Retain(n.allLogTxIds); err != nil {
		n.recoveryComplete = false
		return err
	}
	return nil
}

func (n *Participant) RunRedoLogGCRPC(ctx context.Context, _ sinfonia.RunRedoLogGCRequest) (sinfonia.RunRedoLogGCResponse, error) {
	err := n.RunRedoLogGC(ctx)
	return sinfonia.RunRedoLogGCResponse{Ack: err == nil}, err
}

func (n *Participant) RunForcedAbortGC() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return nil
	}
	current := n.currentEpochLocked()
	if err := n.persistEpochFloorLocked(current); err != nil {
		return err
	}
	_, err := n.forcedAborts.GCByEpoch(current)
	return err
}

func (n *Participant) RunForcedAbortGCRPC(ctx context.Context, _ sinfonia.RunForcedAbortGCRequest) (sinfonia.RunForcedAbortGCResponse, error) {
	err := n.RunForcedAbortGC()
	return sinfonia.RunForcedAbortGCResponse{Ack: err == nil}, err
}

func (n *Participant) SetRedoFsyncHookForTest(fn func(*os.File) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog != nil {
		n.redoLog.SetFsyncHookForTest(fn)
	}
}

func (n *Participant) RedoEntryCountForTest() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog == nil {
		return 0
	}
	return len(n.redoLog.Entries())
}

func (n *Participant) HasAppliedTrackingForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.appliedLocal[txId] {
		return true
	}
	if rem := n.appliedRemote[txId]; len(rem) > 0 {
		return true
	}
	for _, txIds := range n.pendingNotify {
		if _, ok := txIds[txId]; ok {
			return true
		}
	}
	return false
}
