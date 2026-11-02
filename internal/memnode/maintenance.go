package memnode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"time"

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
	if truncateCount == 0 {
		return nil
	}
	removed := entries[:truncateCount]
	if err := n.redoLog.TruncateHead(truncateCount); err != nil {
		n.recoveryComplete = false
		return err
	}
	for _, entry := range removed {
		delete(n.allLogTxIds, entry.TransactionId)
		delete(n.inDoubt, entry.TransactionId)
		delete(n.appliedLocal, entry.TransactionId)
		delete(n.appliedRemote, entry.TransactionId)
		for pid, txIds := range n.pendingNotify {
			delete(txIds, entry.TransactionId)
			if len(txIds) == 0 {
				delete(n.pendingNotify, pid)
			}
		}
	}
	// Leave decisions for epoch-based replay cleanup: a decision may be the
	// only durable outcome evidence when a crash lost the exact reply.
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

func (n *Participant) BeginBackupPhase(ctx context.Context, req sinfonia.BeginBackupPhaseRequest) (sinfonia.BeginBackupPhaseResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.diskImage == nil {
		return sinfonia.BeginBackupPhaseResponse{Ack: true}, nil
	}
	boundary := sinfonia.TransactionID{}
	if n.redoLog != nil {
		boundary = n.redoLog.LastTID()
	}
	n.backupMode = true
	if err := n.diskImage.Sync(); err != nil {
		return sinfonia.BeginBackupPhaseResponse{Ack: false}, err
	}
	return sinfonia.BeginBackupPhaseResponse{Ack: true, BoundaryTID: boundary}, nil
}

func (n *Participant) CompleteBackupPhase(ctx context.Context, req sinfonia.CompleteBackupPhaseRequest) (sinfonia.CompleteBackupPhaseResponse, error) {
	n.mu.Lock()
	if n.diskImage == nil {
		n.backupMode = false
		n.pendingDiskWrites = nil
		n.mu.Unlock()
		return sinfonia.CompleteBackupPhaseResponse{Ack: true}, nil
	}
	snapshotPath := ""
	if n.storageDir != "" {
		snapshotPath = n.storageDir + "/backup-" + req.BackupID + ".img"
	}
	n.mu.Unlock()

	if snapshotPath != "" {
		if err := n.diskImage.Snapshot(snapshotPath); err != nil {
			return sinfonia.CompleteBackupPhaseResponse{Ack: false}, err
		}
	}
	n.mu.Lock()
	pending := append([]sinfonia.WriteItem(nil), n.pendingDiskWrites...)
	n.pendingDiskWrites = nil
	n.backupMode = false
	n.mu.Unlock()

	for _, w := range pending {
		if err := n.diskImage.ApplyWrite(w.Addr, w.Data); err != nil {
			return sinfonia.CompleteBackupPhaseResponse{Ack: false}, err
		}
	}
	snapshotSHA256 := ""
	if snapshotPath != "" {
		snapshot, err := os.Open(snapshotPath)
		if err != nil {
			return sinfonia.CompleteBackupPhaseResponse{Ack: false}, err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, snapshot)
		closeErr := snapshot.Close()
		if copyErr != nil {
			return sinfonia.CompleteBackupPhaseResponse{Ack: false}, copyErr
		}
		if closeErr != nil {
			return sinfonia.CompleteBackupPhaseResponse{Ack: false}, closeErr
		}
		snapshotSHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	return sinfonia.CompleteBackupPhaseResponse{Ack: true, SnapshotPath: snapshotPath, SnapshotSHA256: snapshotSHA256}, nil
}

func (n *Participant) SetRedoBatchingForTest(window time.Duration, max int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog != nil {
		n.redoLog.ConfigureBatchingForTest(window, max)
	}
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
