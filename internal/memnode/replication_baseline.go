package memnode

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

const (
	baselineTransferTempPrefix       = ".baseline-transfer-"
	baselineReplayTransferTempPrefix = ".baseline-transfer-replay-"
	baselineArtifactImage            = "image"
	baselineArtifactReplay           = "replay"
)

func (n *Participant) SetReplicationBaselineBoundaryHookForTest(fn func(phase string) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.baselineInstallBoundaryHook = fn
}

func (n *Participant) callBaselineBoundaryHookLocked(phase string) error {
	if n.baselineInstallBoundaryHook == nil {
		return nil
	}
	return n.baselineInstallBoundaryHook(phase)
}

func (n *Participant) ExportReplicationBaselineChunk(ctx context.Context, req sinfonia.ExportReplicationBaselineChunkRequest) (sinfonia.ExportReplicationBaselineChunkResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplPrimary {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, errors.New("node is not configured as primary")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
	}
	if n.backupMode || n.diskImage == nil {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if req.MaxBytes == 0 || req.MaxBytes > baselineChunkSize {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("baseline chunk size must be between 1 and %d bytes", baselineChunkSize)
	}
	artifact := req.Artifact
	if artifact == "" {
		artifact = baselineArtifactImage
	}
	var total int64
	var readAt func([]byte, int64) error
	var checksum func() ([]byte, error)
	switch artifact {
	case baselineArtifactImage:
		total = int64(n.diskImage.Size())
		readAt = n.diskImage.ReadAt
		checksum = n.diskImage.SHA256
	case baselineArtifactReplay:
		if n.replay == nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, errors.New("replay store unavailable")
		}
		var err error
		total, err = n.replay.Size()
		if err != nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
		}
		readAt = n.replay.ReadAt
		checksum = n.replay.SHA256
	default:
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("unknown replication baseline artifact %q", artifact)
	}
	if total < 0 || uint64(total) > maxBaselineBytes {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("baseline %s size %d exceeds configured maximum %d", artifact, total, maxBaselineBytes)
	}
	if total == 0 {
		if req.Offset != 0 {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("baseline %s chunk offset %d is outside empty artifact", artifact, req.Offset)
		}
		resp := sinfonia.ExportReplicationBaselineChunkResponse{Ack: true, TotalSize: 0}
		if artifact == baselineArtifactImage {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, errors.New("replication image cannot be empty")
		}
		digest, err := checksum()
		if err != nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
		}
		resp.SHA256 = digest
		return resp, nil
	}
	if req.Offset >= uint64(total) {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("baseline %s chunk offset %d is outside artifact size %d", artifact, req.Offset, total)
	}
	length := int(req.MaxBytes)
	if remaining := total - int64(req.Offset); remaining < int64(length) {
		length = int(remaining)
	}
	data := make([]byte, length)
	if err := readAt(data, int64(req.Offset)); err != nil {
		return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
	}
	resp := sinfonia.ExportReplicationBaselineChunkResponse{Ack: true, Data: data, TotalSize: uint64(total)}
	if artifact == baselineArtifactImage && req.Offset == 0 && n.replay != nil {
		replaySize, err := n.replay.Size()
		if err != nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
		}
		replayChecksum, err := n.replay.SHA256()
		if err != nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
		}
		if replaySize < 0 || uint64(replaySize) > maxBaselineBytes || len(replayChecksum) != sha256.Size {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, errors.New("invalid replay baseline manifest")
		}
		resp.ReplaySize = uint64(replaySize)
		resp.ReplaySHA256 = replayChecksum
	}
	if req.Offset == 0 {
		digest, err := checksum()
		if err != nil {
			return sinfonia.ExportReplicationBaselineChunkResponse{Ack: false}, err
		}
		resp.SHA256 = digest
		if artifact == baselineArtifactImage && n.redoLog != nil {
			resp.BoundaryTID = n.redoLog.LastTID()
		}
	}
	return resp, nil
}

func (n *Participant) BeginReplicationBaseline(ctx context.Context, req sinfonia.BeginReplicationBaselineRequest) (sinfonia.BeginReplicationBaselineResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, err
	}
	if n.diskImage == nil || n.storageDir == "" {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if req.SessionID == "" || req.TotalSize == 0 || req.TotalSize != uint64(len(n.memory)) || req.TotalSize > maxBaselineBytes {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, errors.New("invalid replication baseline transfer size or session")
	}
	if len(req.SHA256) != sha256.Size {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, errors.New("replication baseline checksum must be SHA-256")
	}
	replayPresent := len(req.ReplaySHA256) > 0 || req.ReplaySize > 0
	if replayPresent && (req.ReplaySize > maxBaselineBytes || len(req.ReplaySHA256) != sha256.Size) {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, errors.New("invalid replay baseline manifest")
	}
	if n.baselineTransfer != nil {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, errors.New("replication baseline transfer already active")
	}
	file, err := os.CreateTemp(n.storageDir, baselineTransferTempPrefix)
	if err != nil {
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, err
	}
	path := file.Name()
	if err := file.Truncate(int64(req.TotalSize)); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return sinfonia.BeginReplicationBaselineResponse{Ack: false}, err
	}
	var replayFile *os.File
	var replayPath string
	if replayPresent {
		replayFile, err = os.CreateTemp(n.storageDir, baselineReplayTransferTempPrefix)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return sinfonia.BeginReplicationBaselineResponse{Ack: false}, err
		}
		replayPath = replayFile.Name()
		if err := replayFile.Truncate(int64(req.ReplaySize)); err != nil {
			_ = replayFile.Close()
			_ = os.Remove(replayPath)
			_ = file.Close()
			_ = os.Remove(path)
			return sinfonia.BeginReplicationBaselineResponse{Ack: false}, err
		}
	}
	n.baselineTransfer = &baselineTransfer{
		sessionID:       req.SessionID,
		owner:           req.BarrierOwner,
		path:            path,
		file:            file,
		totalSize:       req.TotalSize,
		checksum:        append([]byte(nil), req.SHA256...),
		replayPath:      replayPath,
		replayFile:      replayFile,
		replayTotalSize: req.ReplaySize,
		replayChecksum:  append([]byte(nil), req.ReplaySHA256...),
		replayPresent:   replayPresent,
		boundary:        req.BoundaryTID,
	}
	return sinfonia.BeginReplicationBaselineResponse{Ack: true}, nil
}

func (n *Participant) WriteReplicationBaselineChunk(ctx context.Context, req sinfonia.WriteReplicationBaselineChunkRequest) (sinfonia.WriteReplicationBaselineChunkResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, err
	}
	transfer := n.baselineTransfer
	if transfer == nil || transfer.sessionID != req.SessionID || transfer.owner != req.BarrierOwner {
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, errors.New("unknown replication baseline transfer")
	}
	artifact := req.Artifact
	if artifact == "" {
		artifact = baselineArtifactImage
	}
	var file *os.File
	var nextOffset, totalSize uint64
	switch artifact {
	case baselineArtifactImage:
		file, nextOffset, totalSize = transfer.file, transfer.nextOffset, transfer.totalSize
	case baselineArtifactReplay:
		if !transfer.replayPresent {
			return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, errors.New("replay artifact is not part of this baseline transfer")
		}
		file, nextOffset, totalSize = transfer.replayFile, transfer.replayNextOffset, transfer.replayTotalSize
	default:
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, fmt.Errorf("unknown replication baseline artifact %q", artifact)
	}
	if file == nil || req.Offset != nextOffset || len(req.Data) == 0 || len(req.Data) > baselineChunkSize || req.Offset+uint64(len(req.Data)) > totalSize {
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, errors.New("invalid replication baseline chunk sequence")
	}
	if len(req.SHA256) != sha256.Size || !equalBytes(req.SHA256, sha256Bytes(req.Data)) {
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, errors.New("replication baseline chunk checksum mismatch")
	}
	if _, err := file.WriteAt(req.Data, int64(req.Offset)); err != nil {
		return sinfonia.WriteReplicationBaselineChunkResponse{Ack: false}, err
	}
	if artifact == baselineArtifactImage {
		transfer.nextOffset += uint64(len(req.Data))
	} else {
		transfer.replayNextOffset += uint64(len(req.Data))
	}
	return sinfonia.WriteReplicationBaselineChunkResponse{Ack: true}, nil
}

func (n *Participant) CommitReplicationBaseline(ctx context.Context, req sinfonia.CommitReplicationBaselineRequest) (sinfonia.CommitReplicationBaselineResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
	}
	transfer := n.baselineTransfer
	if transfer == nil || transfer.sessionID != req.SessionID || transfer.owner != req.BarrierOwner {
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, errors.New("unknown replication baseline transfer")
	}
	if transfer.nextOffset != transfer.totalSize || (transfer.replayPresent && transfer.replayNextOffset != transfer.replayTotalSize) {
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, errors.New("replication baseline transfer is incomplete")
	}
	if err := transfer.file.Sync(); err != nil {
		n.abortBaselineTransferLocked()
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
	}
	if err := transfer.file.Close(); err != nil {
		n.abortBaselineTransferLocked()
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
	}
	transfer.file = nil
	checksum, err := fileSHA256(transfer.path)
	if err != nil {
		n.abortBaselineTransferLocked()
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
	}
	if !equalBytes(checksum, transfer.checksum) {
		n.abortBaselineTransferLocked()
		n.replicaSynced = false
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, errors.New("replication baseline checksum mismatch")
	}
	if transfer.replayPresent {
		if err := transfer.replayFile.Sync(); err != nil {
			n.abortBaselineTransferLocked()
			return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
		}
		if err := transfer.replayFile.Close(); err != nil {
			n.abortBaselineTransferLocked()
			return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
		}
		transfer.replayFile = nil
		replayChecksum, err := fileSHA256(transfer.replayPath)
		if err != nil {
			n.abortBaselineTransferLocked()
			return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
		}
		if !equalBytes(replayChecksum, transfer.replayChecksum) {
			n.abortBaselineTransferLocked()
			n.replicaSynced = false
			return sinfonia.CommitReplicationBaselineResponse{Ack: false}, errors.New("replay baseline checksum mismatch")
		}
	}
	path := transfer.path
	replayPath := transfer.replayPath
	boundary := transfer.boundary
	if _, err := n.installReplicationBaselineFileLocked(req.BarrierOwner, path, replayPath, boundary); err != nil {
		n.abortBaselineTransferLocked()
		return sinfonia.CommitReplicationBaselineResponse{Ack: false}, err
	}
	_ = os.Remove(path)
	if replayPath != "" {
		_ = os.Remove(replayPath)
	}
	n.baselineTransfer = nil
	return sinfonia.CommitReplicationBaselineResponse{Ack: true, BoundaryTID: boundary}, nil
}

func (n *Participant) AbortReplicationBaseline(ctx context.Context, req sinfonia.AbortReplicationBaselineRequest) (sinfonia.AbortReplicationBaselineResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.baselineTransfer == nil {
		return sinfonia.AbortReplicationBaselineResponse{Ack: true}, nil
	}
	if n.baselineTransfer.sessionID != req.SessionID || n.baselineTransfer.owner != req.BarrierOwner {
		return sinfonia.AbortReplicationBaselineResponse{Ack: false}, ErrReplicationBaselineLease
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.AbortReplicationBaselineResponse{Ack: false}, err
	}
	n.abortBaselineTransferLocked()
	return sinfonia.AbortReplicationBaselineResponse{Ack: true}, nil
}

func (n *Participant) abortBaselineTransferLocked() {
	if n.baselineTransfer == nil {
		return
	}
	if n.baselineTransfer.file != nil {
		_ = n.baselineTransfer.file.Close()
	}
	_ = os.Remove(n.baselineTransfer.path)
	if n.baselineTransfer.replayFile != nil {
		_ = n.baselineTransfer.replayFile.Close()
	}
	if n.baselineTransfer.replayPath != "" {
		_ = os.Remove(n.baselineTransfer.replayPath)
	}
	n.baselineTransfer = nil
}

func (n *Participant) installReplicationBaselineFileLocked(owner, path, replayPath string, boundary sinfonia.TransactionID) (sinfonia.InstallReplicationBaselineResponse, error) {
	return n.installReplicationBaselineLocked(owner, nil, path, replayPath, boundary)
}

func (n *Participant) installReplicationBaselineBytesLocked(owner string, image, checksum []byte, boundary sinfonia.TransactionID) (sinfonia.InstallReplicationBaselineResponse, error) {
	if len(checksum) != sha256.Size || !equalBytes(checksum, sha256Bytes(image)) {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, errors.New("replication baseline checksum mismatch")
	}
	return n.installReplicationBaselineLocked(owner, image, "", "", boundary)
}

func (n *Participant) installReplicationBaselineLocked(owner string, image []byte, stagedPath, stagedReplayPath string, boundary sinfonia.TransactionID) (sinfonia.InstallReplicationBaselineResponse, error) {
	if err := n.touchGlobalLockLocked(owner); err != nil {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	n.baselineRecoveryRequired = true
	journal, err := storage.BeginBaselineInstall(n.storageDir)
	if err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if err := n.callBaselineBoundaryHookLocked("journal-created"); err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if stagedPath != "" {
		err = n.diskImage.ReplaceFrom(stagedPath)
	} else {
		err = n.diskImage.Replace(image)
	}
	if err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if err := n.callBaselineBoundaryHookLocked("image-synced"); err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if stagedReplayPath != "" {
		if n.replay == nil {
			err := errors.New("replay store unavailable")
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.replay.ReplaceFrom(stagedReplayPath); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.callBaselineBoundaryHookLocked("replay-synced"); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	}
	if n.redoLog != nil {
		if err := n.redoLog.Reset(); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.callBaselineBoundaryHookLocked("redo-reset"); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	}
	if n.decided != nil {
		if err := n.decided.Reset(); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.callBaselineBoundaryHookLocked("decided-reset"); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.Reset(); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.callBaselineBoundaryHookLocked("forced-abort-reset"); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	}
	if n.storageDir != "" {
		if err := storage.StoreProcessedPointer(n.storageDir, 0); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
		if err := n.callBaselineBoundaryHookLocked("processed-pointer-reset"); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	}
	if err := n.callBaselineBoundaryHookLocked("before-commit"); err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if err := journal.Commit(); err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if err := n.callBaselineBoundaryHookLocked("commit-published"); err != nil {
		n.markBaselineInstallFailedLocked(err)
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if stagedPath != "" {
		if err := n.diskImage.ReadAt(n.memory, 0); err != nil {
			n.markBaselineInstallFailedLocked(err)
			return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
		}
	} else {
		copy(n.memory, image)
	}
	n.resetBaselineMemoryStateLocked()
	n.baselineRecoveryRequired = false
	return sinfonia.InstallReplicationBaselineResponse{Ack: true, BoundaryTID: boundary}, nil
}

func (n *Participant) resetBaselineMemoryStateLocked() {
	n.recoveryComplete = true
	n.inDoubt = map[sinfonia.TransactionID]*preparedTxn{}
	n.allLogTxIds = map[sinfonia.TransactionID]struct{}{}
	n.appliedLocal = map[sinfonia.TransactionID]bool{}
	n.appliedRemote = map[sinfonia.TransactionID]map[uint32]bool{}
	n.pendingNotify = map[uint32]map[sinfonia.TransactionID]struct{}{}
	n.logParticipants = map[sinfonia.TransactionID][]uint32{}
	n.prepareOutcomes = map[sinfonia.TransactionID]prepareOutcome{}
	n.onePCOutcomes = map[sinfonia.TransactionID]onePCOutcome{}
	n.commitOutcomes = map[sinfonia.TransactionID]commitOutcome{}
	n.timelines = map[sinfonia.TransactionID][]sinfonia.TxnTimelineEvent{}
	n.backupMode = false
	n.pendingDiskWrites = nil
	n.lastBackupBoundary = sinfonia.TransactionID{}
	n.replicaSynced = false
	n.metrics.inDoubtCount.Set(0)
	n.metrics.redoLogSizeBytes.Set(0)
	n.metrics.replicaLagEntries.Set(1)
	n.setReplicationErrorLocked(nil)
}

func (n *Participant) markBaselineInstallFailedLocked(err error) {
	n.baselineRecoveryRequired = true
	n.recoveryComplete = false
	n.replicaSynced = false
	n.setReplicationErrorLocked(err)
}

func (n *Participant) replicationBaselineIngressBlockedLocked() bool {
	// Drop the staged transfer after failure, but keep the node unavailable.
	// Reopening will restore the old files from the rollback journal.
	return n.baselineTransfer != nil || n.baselineRecoveryRequired
}

func (n *Participant) baselineTransferChunkLimit() int {
	return baselineChunkSize
}

func fileSHA256(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, baselineChunkSize)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func sha256Bytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
