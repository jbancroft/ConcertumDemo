package memnode

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

var ErrReplicationBaselineRequiresGlobalLock = errors.New("replication baseline requires the global write lock")

var ErrReplicationBaselineLease = errors.New("replication baseline global lock lease is invalid")

var ErrReplicationBaselineInProgress = errors.New("replication baseline installation in progress")

type baselineTransfer struct {
	sessionID        string
	owner            string
	path             string
	file             *os.File
	totalSize        uint64
	nextOffset       uint64
	checksum         []byte
	replayPath       string
	replayFile       *os.File
	replayTotalSize  uint64
	replayNextOffset uint64
	replayChecksum   []byte
	replayPresent    bool
	boundary         sinfonia.TransactionID
	epochFloor       uint64
}

type PrimarySyncPeer interface {
	LockAllBlockingOwned(ctx context.Context, owner string) error
	UnlockAllOwned(owner string)
	ExportReplicationBaselineChunk(ctx context.Context, req sinfonia.ExportReplicationBaselineChunkRequest) (sinfonia.ExportReplicationBaselineChunkResponse, error)
	ListCommittedTIDs(ctx context.Context, req sinfonia.ListCommittedTIDsRequest) (sinfonia.ListCommittedTIDsResponse, error)
	ListRedoEntries(ctx context.Context, req sinfonia.ListRedoEntriesRequest) (sinfonia.ListRedoEntriesResponse, error)
}

// ExportReplicationBaseline returns the primary's saved memory image.
// The manager must hold the global write lock so updates can't race the copy.
func (n *Participant) ExportReplicationBaseline(ctx context.Context, req sinfonia.ExportReplicationBaselineRequest) (sinfonia.ExportReplicationBaselineResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplPrimary {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, errors.New("node is not configured as primary")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, err
	}
	if n.backupMode {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, errors.New("replication baseline unavailable during backup phase")
	}
	if n.diskImage == nil {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if err := n.diskImage.Sync(); err != nil {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, err
	}
	image, err := n.diskImage.Bytes()
	if err != nil {
		return sinfonia.ExportReplicationBaselineResponse{Ack: false}, err
	}
	checksum := sha256.Sum256(image)
	boundary := sinfonia.TransactionID{}
	if n.redoLog != nil {
		boundary = n.redoLog.LastTID()
	}
	return sinfonia.ExportReplicationBaselineResponse{Ack: true, Image: image, SHA256: checksum[:], BoundaryTID: boundary, EpochFloor: n.epochFloor}, nil
}

// InstallReplicationBaseline replaces a replica's image and old transaction state.
// The manager must hold the replica's global write lock. Saved replies stay local;
// this call doesn't transfer them from the primary.
func (n *Participant) InstallReplicationBaseline(ctx context.Context, req sinfonia.InstallReplicationBaselineRequest) (sinfonia.InstallReplicationBaselineResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if n.backupMode {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, errors.New("replication baseline unavailable during backup phase")
	}
	if n.diskImage == nil {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if len(req.Image) != len(n.memory) {
		return sinfonia.InstallReplicationBaselineResponse{Ack: false}, fmt.Errorf("replication baseline size %d does not match memory size %d", len(req.Image), len(n.memory))
	}
	return n.installReplicationBaselineBytesLocked(req.BarrierOwner, req.Image, req.SHA256, req.BoundaryTID, req.EpochFloor)
}

func (n *Participant) SyncFromPrimary(ctx context.Context, primary PrimarySyncPeer) error {
	if primary == nil {
		return errors.New("primary peer required")
	}
	owner := fmt.Sprintf("sync/%p", n)
	if err := primary.LockAllBlockingOwned(ctx, owner); err != nil {
		return err
	}
	primaryLocked := true
	defer func() {
		if primaryLocked {
			primary.UnlockAllOwned(owner)
		}
	}()
	if err := n.LockAllBlockingOwned(ctx, owner); err != nil {
		return err
	}
	replicaLocked := true
	defer func() {
		if replicaLocked {
			n.UnlockAllOwned(owner)
		}
	}()
	first, err := primary.ExportReplicationBaselineChunk(ctx, sinfonia.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "image", Offset: 0, MaxBytes: baselineChunkSize})
	if err != nil || !first.Ack {
		if err != nil {
			return err
		}
		return errors.New("primary did not acknowledge replication baseline chunk")
	}
	if first.TotalSize == 0 || first.TotalSize != uint64(len(n.memory)) || len(first.SHA256) != sha256.Size || len(first.Data) == 0 {
		return errors.New("primary returned an invalid replication baseline manifest")
	}
	replayPresent := len(first.ReplaySHA256) > 0 || first.ReplaySize > 0
	if replayPresent && (first.ReplaySize > maxBaselineBytes || len(first.ReplaySHA256) != sha256.Size) {
		return errors.New("primary returned an invalid replay baseline manifest")
	}
	sessionID := fmt.Sprintf("%s/%d", owner, time.Now().UnixNano())
	begin, err := n.BeginReplicationBaseline(ctx, sinfonia.BeginReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID, TotalSize: first.TotalSize, SHA256: first.SHA256, ReplaySize: first.ReplaySize, ReplaySHA256: first.ReplaySHA256, BoundaryTID: first.BoundaryTID, EpochFloor: first.EpochFloor})
	if err != nil || !begin.Ack {
		if err != nil {
			return err
		}
		return errors.New("replica did not acknowledge replication baseline begin")
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = n.AbortReplicationBaseline(context.Background(), sinfonia.AbortReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
		}
	}()
	chunk := first
	offset := uint64(0)
	for {
		if chunk.TotalSize != first.TotalSize || len(chunk.Data) == 0 || len(chunk.Data) > baselineChunkSize || offset+uint64(len(chunk.Data)) > first.TotalSize {
			return errors.New("invalid replication baseline chunk sequence")
		}
		digest := sha256.Sum256(chunk.Data)
		written, err := n.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: sessionID, Artifact: "image", Offset: offset, SHA256: digest[:], Data: chunk.Data})
		if err != nil || !written.Ack {
			if err != nil {
				return err
			}
			return errors.New("replica did not acknowledge replication baseline chunk")
		}
		offset += uint64(len(chunk.Data))
		if offset == first.TotalSize {
			break
		}
		chunk, err = primary.ExportReplicationBaselineChunk(ctx, sinfonia.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "image", Offset: offset, MaxBytes: baselineChunkSize})
		if err != nil || !chunk.Ack {
			if err != nil {
				return err
			}
			return errors.New("primary did not acknowledge replication baseline chunk")
		}
	}
	if replayPresent {
		replayFirst, err := primary.ExportReplicationBaselineChunk(ctx, sinfonia.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "replay", Offset: 0, MaxBytes: baselineChunkSize})
		if err != nil {
			return err
		}
		if err := syncBaselineArtifact(ctx, primary, n, owner, sessionID, "replay", replayFirst, first.ReplaySize, first.ReplaySHA256); err != nil {
			return err
		}
	}
	installed, err := n.CommitReplicationBaseline(ctx, sinfonia.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
	if err != nil || !installed.Ack {
		if err != nil {
			return err
		}
		return errors.New("replica did not acknowledge replication baseline commit")
	}
	committed = true
	n.mu.Lock()
	n.replicaSynced = true
	n.mu.Unlock()
	return nil
}

func syncBaselineArtifact(ctx context.Context, primary PrimarySyncPeer, replica *Participant, owner, sessionID, artifact string, first sinfonia.ExportReplicationBaselineChunkResponse, total uint64, checksum []byte) error {
	if total == 0 {
		if first.Ack && first.TotalSize == 0 && len(first.Data) == 0 && len(first.SHA256) == sha256.Size && equalBytes(first.SHA256, checksum) {
			return nil
		}
		return errors.New("primary returned an invalid empty baseline artifact")
	}
	if !first.Ack || first.TotalSize != total || len(first.SHA256) != sha256.Size || !equalBytes(first.SHA256, checksum) || len(first.Data) == 0 || len(first.Data) > baselineChunkSize || uint64(len(first.Data)) > total {
		return fmt.Errorf("primary returned an invalid %s baseline artifact", artifact)
	}
	offset := uint64(0)
	chunk := first
	for {
		if chunk.TotalSize != total || len(chunk.Data) == 0 || len(chunk.Data) > baselineChunkSize || offset+uint64(len(chunk.Data)) > total {
			return fmt.Errorf("invalid %s baseline artifact chunk sequence", artifact)
		}
		digest := sha256.Sum256(chunk.Data)
		written, err := replica.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: sessionID, Artifact: artifact, Offset: offset, SHA256: digest[:], Data: chunk.Data})
		if err != nil {
			return err
		}
		if !written.Ack {
			return fmt.Errorf("replica did not acknowledge %s baseline artifact chunk", artifact)
		}
		offset += uint64(len(chunk.Data))
		if offset == total {
			return nil
		}
		chunk, err = primary.ExportReplicationBaselineChunk(ctx, sinfonia.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: artifact, Offset: offset, MaxBytes: baselineChunkSize})
		if err != nil {
			return err
		}
		if !chunk.Ack {
			return fmt.Errorf("primary did not acknowledge %s baseline artifact chunk", artifact)
		}
	}
}
