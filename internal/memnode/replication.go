package memnode

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

type ReplicationMode int

const (
	ModeSingle ReplicationMode = iota
	ModeReplPrimary
	ModeReplReplica
)

var ErrReplicaRejectsCoordinator = errors.New("replica rejects direct coordinator requests")

var ErrReplicationRequiresDurability = errors.New("replication requires durable storage")

var ErrStaleGeneration = errors.New("stale or missing fencing generation")

type ReplicaPeer interface {
	ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error)
	ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error)
	ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error)
}

func (n *Participant) SetPeerDialer(tlsConfig *tls.Config, timeout time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerTLS = tlsConfig
	if timeout > 0 {
		n.peerTimeout = timeout
	}
}

func (n *Participant) Generation() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.generation
}

func (n *Participant) checkGenerationLocked(generation uint64) error {
	if n.generation == 0 {
		return nil
	}
	if generation == 0 || generation != n.generation {
		return ErrStaleGeneration
	}
	return nil
}

func (n *Participant) replicationRoleLocked() sinfonia.ReplicationRole {
	switch n.mode {
	case ModeReplPrimary:
		return sinfonia.ReplicationRolePrimary
	case ModeReplReplica:
		return sinfonia.ReplicationRoleReplica
	default:
		return sinfonia.ReplicationRoleSingle
	}
}

func (n *Participant) setReplicationErrorLocked(err error) {
	if err == nil {
		n.lastReplicationError = ""
		return
	}
	n.lastReplicationError = err.Error()
}

func (n *Participant) newRPCPeerLocked(addr string) *RPCPeer {
	return NewRPCPeer(addr, n.peerTLS, n.peerTimeout)
}

func (n *Participant) configureAsPrimaryLocked(replicaID uint32, replica ReplicaPeer) {
	n.mode = ModeReplPrimary
	n.replicaID = replicaID
	n.replica = replica
	n.primaryID = 0
	n.replicaSynced = true
	n.metrics.replicaLagEntries.Set(0)
}

func (n *Participant) ConfigureAsPrimary(replicaID uint32, replica ReplicaPeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configureAsPrimaryLocked(replicaID, replica)
}

func (n *Participant) configureAsReplicaLocked(primaryID uint32) {
	n.mode = ModeReplReplica
	n.primaryID = primaryID
	n.replica = nil
	n.replicaID = 0
	n.replicaSynced = false
	n.metrics.replicaLagEntries.Set(1)
}

func (n *Participant) ConfigureAsReplica(primaryID uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configureAsReplicaLocked(primaryID)
}

func (n *Participant) PromoteToPrimary(replicaID uint32, replica ReplicaPeer) {
	n.ConfigureAsPrimary(replicaID, replica)
	n.mu.Lock()
	n.replicaSynced = true
	n.mu.Unlock()
}

func (n *Participant) RecordPrimaryHeartbeat() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.heartbeat = time.Now()
}

func (n *Participant) PrimaryLikelyDown(timeout time.Duration) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica || timeout <= 0 {
		return false
	}
	if n.heartbeat.IsZero() {
		return false
	}
	return time.Since(n.heartbeat) > timeout
}

func (n *Participant) ConfigureReplication(ctx context.Context, req sinfonia.ConfigureReplicationRequest) (sinfonia.ConfigureReplicationResponse, error) {

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ConfigureReplicationResponse{}, n.restartRequired
	}

	switch req.Role {
	case sinfonia.ReplicationRoleSingle:
	case sinfonia.ReplicationRolePrimary:
		if n.redoLog == nil {
			return sinfonia.ConfigureReplicationResponse{Ack: false}, ErrReplicationRequiresDurability
		}
		if req.PartnerRPC == "" {
			return sinfonia.ConfigureReplicationResponse{Ack: false}, errors.New("primary replication requires partner_rpc")
		}
	case sinfonia.ReplicationRoleReplica:
		if n.redoLog == nil {
			return sinfonia.ConfigureReplicationResponse{Ack: false}, ErrReplicationRequiresDurability
		}
		if req.PartnerRPC == "" {
			return sinfonia.ConfigureReplicationResponse{Ack: false}, errors.New("replica replication requires partner_rpc")
		}
	default:
		return sinfonia.ConfigureReplicationResponse{Ack: false}, errors.New("unknown replication role")
	}
	if n.generation != 0 && req.Generation < n.generation {
		return sinfonia.ConfigureReplicationResponse{Ack: false}, ErrStaleGeneration
	}
	if req.Generation > n.generation {
		if n.storageDir != "" {
			if err := persistFencingGenerationWithHook(n.storageDir, req.Generation, n.fencingPublicationHook); err != nil {
				if isFencingPublicationOutcomeUnknown(err) {
					n.generation = req.Generation
					return sinfonia.ConfigureReplicationResponse{Ack: false, OutcomeUnknown: true, Generation: n.generation}, nil
				}
				return sinfonia.ConfigureReplicationResponse{Ack: false}, err
			}
		}
		n.generation = req.Generation
	}

	switch req.Role {
	case sinfonia.ReplicationRoleSingle:
		n.mode = ModeSingle
		n.replica = nil
		n.replicaID = 0
		n.primaryID = 0
		n.partnerRPC = ""
		n.partnerHTTP = ""
		n.replicaSynced = true
		n.metrics.replicaLagEntries.Set(0)
	case sinfonia.ReplicationRolePrimary:
		n.configureAsPrimaryLocked(0, n.newRPCPeerLocked(req.PartnerRPC))
		n.partnerRPC = req.PartnerRPC
		n.partnerHTTP = req.PartnerHTTP
	case sinfonia.ReplicationRoleReplica:
		n.configureAsReplicaLocked(0)
		n.partnerRPC = req.PartnerRPC
		n.partnerHTTP = req.PartnerHTTP
		n.replicaSynced = req.MarkSynced
		if req.MarkSynced {
			n.metrics.replicaLagEntries.Set(0)
		}
	}
	n.setReplicationErrorLocked(nil)
	return sinfonia.ConfigureReplicationResponse{Ack: true, Generation: n.generation}, nil
}

func (n *Participant) ReplicationStatus(ctx context.Context, _ sinfonia.ReplicationStatusRequest) (sinfonia.ReplicationStatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return sinfonia.ReplicationStatusResponse{
		Role:          n.replicationRoleLocked(),
		Ready:         n.isReadyLocked(),
		ReplicaSynced: n.replicaSynced,
		PartnerRPC:    n.partnerRPC,
		PartnerHTTP:   n.partnerHTTP,
		LastError:     n.lastReplicationError,
		Generation:    n.generation,
	}, nil
}

// replicateReplayRecordLocked saves a reply on the replica before returning it
// to the caller. Release the primary's mutex during the RPC so the replica can
// take its own lock without deadlocking.
func (n *Participant) replicateReplayRecordLocked(ctx context.Context, txId sinfonia.TransactionID, kind sinfonia.ReplayRecordKind, prepare *sinfonia.ExecPrepareResponse, onePC *sinfonia.ExecCommit1PCResponse) error {
	if n.restartRequired != nil {
		return n.restartRequired
	}
	if n.mode != ModeReplPrimary || n.replica == nil {
		return nil
	}
	req := sinfonia.ReplicateReplayRecordRequest{TID: txId, Kind: kind}
	if prepare != nil {
		cloned := clonePrepareResponse(*prepare)
		req.Prepare = &cloned
	}
	if onePC != nil {
		cloned := clone1PCResponse(*onePC)
		req.OnePC = &cloned
	}
	peer := n.replica
	n.mu.Unlock()
	start := time.Now()
	replResp, replErr := peer.ReplicateReplayRecord(ctx, req)
	n.metrics.replicationRoundtripLatency.Observe(time.Since(start))
	n.mu.Lock()
	if n.restartRequired != nil {
		return n.restartRequired
	}
	if replErr != nil || !replResp.Ack {
		n.metrics.replicationFailures.Inc()
		if replErr != nil {
			n.setReplicationErrorLocked(replErr)
			return replErr
		}
		err := errors.New("replication replay record not acknowledged")
		n.setReplicationErrorLocked(err)
		return err
	}
	n.setReplicationErrorLocked(nil)
	return nil
}

func (n *Participant) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ReplicateRedoEntryResponse{}, n.restartRequired
	}
	if n.mode != ModeReplReplica {
		return sinfonia.ReplicateRedoEntryResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return sinfonia.ReplicateRedoEntryResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	if n.redoLog == nil {
		return sinfonia.ReplicateRedoEntryResponse{Ack: false}, errors.New("redo log unavailable")
	}
	correlationID := req.Entry.TID.CorrelationID()
	txn := &preparedTxn{writes: cloneWrites(req.Entry.WriteItems), participants: append([]uint32(nil), req.Entry.Participants...), preparedAt: n.nowFn(), correlationID: correlationID}
	n.inDoubt[req.Entry.TID] = txn
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
	if err := n.redoLog.AddItem(storage.RedoLogItem{TransactionId: req.Entry.TID, HostIds: txn.participants, WriteItems: txn.writes}); err != nil {
		return sinfonia.ReplicateRedoEntryResponse{}, n.failUncertainLocked("append replicated redo", err)
	}
	n.allLogTxIds[req.Entry.TID] = struct{}{}
	n.logParticipants[req.Entry.TID] = append([]uint32(nil), req.Entry.Participants...)
	txn.redoLoggedTid = true
	n.recordTimelineLocked(req.Entry.TID, correlationID, "replicated_prepare", "")
	return sinfonia.ReplicateRedoEntryResponse{Ack: true}, nil
}

func (n *Participant) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ReplicateCommitDecisionResponse{}, n.restartRequired
	}
	if n.mode != ModeReplReplica {
		return sinfonia.ReplicateCommitDecisionResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return sinfonia.ReplicateCommitDecisionResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	resp, err := n.commitLocked(sinfonia.CommitRequest{TID: req.TID, ActionCommit: req.ActionCommit})
	if err != nil {
		return sinfonia.ReplicateCommitDecisionResponse{}, n.failUncertainLocked("apply replicated decision", err)
	}
	return sinfonia.ReplicateCommitDecisionResponse{Ack: resp.Ack}, nil
}

// ReplicateReplayRecord saves a reply so a promoted replica can answer a retry
// with the same transaction ID. Saving a reply doesn't execute the transaction.
func (n *Participant) ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restartRequired != nil {
		return sinfonia.ReplicateReplayRecordResponse{}, n.restartRequired
	}
	if n.mode != ModeReplReplica {
		return sinfonia.ReplicateReplayRecordResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return sinfonia.ReplicateReplayRecordResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	if n.replay == nil {
		return sinfonia.ReplicateReplayRecordResponse{Ack: false}, errors.New("replay store unavailable")
	}
	switch req.Kind {
	case sinfonia.ReplayRecordPrepare:
		if req.Prepare == nil || req.OnePC != nil {
			return sinfonia.ReplicateReplayRecordResponse{Ack: false}, errors.New("invalid prepare replay record")
		}
		if err := n.replay.AppendPrepare(req.TID, *req.Prepare); err != nil {
			return sinfonia.ReplicateReplayRecordResponse{Ack: false}, err
		}
		n.prepareOutcomes[req.TID] = prepareOutcome{resp: clonePrepareResponse(*req.Prepare)}
	case sinfonia.ReplayRecordOnePC:
		if req.OnePC == nil || req.Prepare != nil {
			return sinfonia.ReplicateReplayRecordResponse{Ack: false}, errors.New("invalid one-phase replay record")
		}
		if err := n.replay.AppendOnePC(req.TID, *req.OnePC); err != nil {
			return sinfonia.ReplicateReplayRecordResponse{Ack: false}, err
		}
		n.onePCOutcomes[req.TID] = onePCOutcome{resp: clone1PCResponse(*req.OnePC)}
	default:
		return sinfonia.ReplicateReplayRecordResponse{Ack: false}, fmt.Errorf("unknown replay record kind %q", req.Kind)
	}
	return sinfonia.ReplicateReplayRecordResponse{Ack: true}, nil
}

func (n *Participant) ListCommittedTIDs(ctx context.Context, _ sinfonia.ListCommittedTIDsRequest) (sinfonia.ListCommittedTIDsResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := sinfonia.ListCommittedTIDsResponse{}
	if n.decided == nil || n.redoLog == nil {
		return resp, nil
	}
	for _, entry := range n.redoLog.Entries() {
		if c, ok := n.decided.Get(entry.TransactionId); ok && c {
			resp.TIDs = append(resp.TIDs, entry.TransactionId)
		}
	}
	return resp, nil
}

func (n *Participant) ListRedoEntries(ctx context.Context, _ sinfonia.ListRedoEntriesRequest) (sinfonia.ListRedoEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := sinfonia.ListRedoEntriesResponse{}
	if n.redoLog == nil {
		return resp, nil
	}
	for _, entry := range n.redoLog.Entries() {
		resp.Entries = append(resp.Entries, sinfonia.RedoEntryPayload{TID: entry.TransactionId, Participants: append([]uint32(nil), entry.HostIds...), WriteItems: cloneWrites(entry.WriteItems)})
	}
	return resp, nil
}

func (n *Participant) SetGenerationForTest(generation uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.generation = generation
}
