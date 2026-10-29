package sinfonia

// RequestID labels a request delivery attempt within a transaction.
type RequestID string

// ExecPrepareRequest asks a participant to check and prepare its part of a transaction.
type ExecPrepareRequest struct {
	TID           TransactionID
	RequestID     RequestID
	CorrelationID string
	Participants  []uint32
	HasWrites     bool
	CmpItems      []CmpItem
	ReadItems     []ReadItem
	WriteItems    []WriteItem
	Epoch         uint64
	Class         string
	Generation    uint64
}

// ExecPrepareResponse returns the participant's vote and any bytes it read.
type ExecPrepareResponse struct {
	RequestID    RequestID
	Vote         Vote
	ReadData     map[uint32][]byte
	CurrentEpoch uint64
	LockConflict *LockConflict
}

// CommitRequest tells a participant to commit or abort its prepared work.
type CommitRequest struct {
	TID           TransactionID
	RequestID     RequestID
	CorrelationID string
	ActionCommit  bool
	Generation    uint64
}

// CommitResponse acknowledges a commit or abort request.
type CommitResponse struct {
	RequestID RequestID
	Ack       bool
}

// ExecCommit1PCRequest combines execution and commit for a single participant.
type ExecCommit1PCRequest struct {
	TID           TransactionID
	RequestID     RequestID
	CorrelationID string
	CmpItems      []CmpItem
	ReadItems     []ReadItem
	WriteItems    []WriteItem
	Epoch         uint64
	Class         string
	Generation    uint64
}

type ExecCommit1PCResponse struct {
	RequestID    RequestID
	Vote         Vote
	ReadData     map[uint32][]byte
	CurrentEpoch uint64
	LockConflict *LockConflict
}

type LockConflict struct {
	Reason         string
	RequestedStart uint64
	RequestedEnd   uint64
	RequestedWrite bool
	HeldStart      uint64
	HeldEnd        uint64
	HeldWrite      bool
}

type CommitBatchRequest struct {
	Decisions []CommitRequest
}

type CommitBatchResponse struct {
	Acks []bool
}

type ReplicateRedoEntryRequest struct {
	Entry RedoEntryPayload
}

type ReplicateRedoEntryResponse struct {
	Ack bool
}

type ReplicateCommitDecisionRequest struct {
	TID          TransactionID
	ActionCommit bool
}

type ReplicateCommitDecisionResponse struct {
	Ack bool
}

// ReplayRecordKind says which kind of reply is being saved. Replies are keyed
// by (TID, Kind), since prepare and single-participant calls have separate paths.
type ReplayRecordKind string

const (
	ReplayRecordPrepare ReplayRecordKind = "prepare"
	ReplayRecordOnePC   ReplayRecordKind = "one_pc"
)

// ReplicateReplayRecordRequest sends a reply to a synced replica. The replica
// saves it before acknowledging, so it can return the same vote and data if it
// takes over and receives a retry.
type ReplicateReplayRecordRequest struct {
	TID     TransactionID
	Kind    ReplayRecordKind
	Prepare *ExecPrepareResponse
	OnePC   *ExecCommit1PCResponse
}

type ReplicateReplayRecordResponse struct {
	Ack bool
}

type ReplicationRole string

const (
	ReplicationRoleSingle  ReplicationRole = "single"
	ReplicationRolePrimary ReplicationRole = "primary"
	ReplicationRoleReplica ReplicationRole = "replica"
)

// ReplicationFencingProtocolVersion identifies managers that understand an
// uncertain result when saving a fencing generation. Durable nodes require it
// before accepting a new generation.
const ReplicationFencingProtocolVersion uint16 = 1

type ConfigureReplicationRequest struct {
	Role                   ReplicationRole
	PartnerRPC             string
	PartnerHTTP            string
	MarkSynced             bool
	Generation             uint64
	FencingProtocolVersion uint16
}

type ConfigureReplicationResponse struct {
	Ack            bool
	OutcomeUnknown bool
	Generation     uint64
}

type ReplicationStatusRequest struct{}

type ReplicationStatusResponse struct {
	Role          ReplicationRole
	Ready         bool
	ReplicaSynced bool
	PartnerRPC    string
	PartnerHTTP   string
	LastError     string
	Generation    uint64
}

type ListCommittedTIDsRequest struct{}

type ListCommittedTIDsResponse struct {
	TIDs []TransactionID
}

type ListRedoEntriesRequest struct{}

type RedoEntryPayload struct {
	TID          TransactionID
	Participants []uint32
	WriteItems   []WriteItem
}

type ListRedoEntriesResponse struct {
	Entries []RedoEntryPayload
}

// ExportReplicationBaselineResponse carries a saved image copied while writes
// are blocked. BoundaryTID helps diagnose where the copy was taken; the image
// itself supplies the replica's state.
type ExportReplicationBaselineRequest struct {
	BarrierOwner string
}

type ExportReplicationBaselineResponse struct {
	Ack         bool
	Image       []byte
	SHA256      []byte
	BoundaryTID TransactionID
	EpochFloor  uint64
}

type InstallReplicationBaselineRequest struct {
	BarrierOwner string
	Image        []byte
	SHA256       []byte
	BoundaryTID  TransactionID
	EpochFloor   uint64
}

type InstallReplicationBaselineResponse struct {
	Ack         bool
	BoundaryTID TransactionID
}

type ExportReplicationBaselineChunkRequest struct {
	BarrierOwner string
	// Artifact selects the saved memory image (empty or "image") or the
	// saved reply log ("replay") in this transfer session.
	Artifact string
	Offset   uint64
	MaxBytes uint32
}

type ExportReplicationBaselineChunkResponse struct {
	Ack          bool
	Data         []byte
	SHA256       []byte
	TotalSize    uint64
	ReplaySize   uint64
	ReplaySHA256 []byte
	BoundaryTID  TransactionID
	EpochFloor   uint64
}

type BeginReplicationBaselineRequest struct {
	BarrierOwner string
	SessionID    string
	TotalSize    uint64
	SHA256       []byte
	ReplaySize   uint64
	ReplaySHA256 []byte
	BoundaryTID  TransactionID
	EpochFloor   uint64
}

type BeginReplicationBaselineResponse struct {
	Ack bool
}

type WriteReplicationBaselineChunkRequest struct {
	BarrierOwner string
	SessionID    string
	Artifact     string
	Offset       uint64
	SHA256       []byte
	Data         []byte
}

type WriteReplicationBaselineChunkResponse struct {
	Ack bool
}

type CommitReplicationBaselineRequest struct {
	BarrierOwner string
	SessionID    string
}

type CommitReplicationBaselineResponse struct {
	Ack         bool
	BoundaryTID TransactionID
}

type AbortReplicationBaselineRequest struct {
	BarrierOwner string
	SessionID    string
}

type AbortReplicationBaselineResponse struct {
	Ack bool
}

// RecoverForceAbortRequest asks a node to record an abort unless it already voted yes.
type RecoverForceAbortRequest struct {
	TID           TransactionID
	Epoch         uint64
	CorrelationID string
}

// RecoverForceAbortResponse returns the participant's vote after that check.
type RecoverForceAbortResponse struct {
	Vote           Vote
	KnownDecision  bool
	DecisionCommit bool
}

// RecoverDecideRequest asks the node to apply a recovery decision.
type RecoverDecideRequest struct {
	TID           TransactionID
	ActionCommit  bool
	CorrelationID string
}

// RecoverDecideResponse acknowledges that recovery decision.
type RecoverDecideResponse struct {
	Ack bool
}

type QueryInDoubtRequest struct{}

type InDoubtEntry struct {
	TID           TransactionID
	PreparedAt    int64
	CorrelationID string
	Participants  []uint32
}

type TxnTimelineEvent struct {
	AtNanos       int64  `json:"at_nanos"`
	Component     string `json:"component"`
	Event         string `json:"event"`
	CorrelationID string `json:"correlation_id"`
	Details       string `json:"details,omitempty"`
}

type QueryInDoubtResponse struct {
	Entries []InDoubtEntry
}

type TidOutcome int32

const (
	TidOutcomeUnknown TidOutcome = iota
	TidOutcomeCommit
	TidOutcomeAbort
)

type QueryTidStatusRequest struct {
	TID TransactionID
}

type QueryTidStatusResponse struct {
	KnownDecision bool
	Outcome       TidOutcome
	VotedOK       bool
}

type NotifyAppliedRequest struct {
	FromMemID uint32
	TIDs      []TransactionID
}

type NotifyAppliedResponse struct {
	Ack bool
}

type RunRedoLogGCRequest struct{}

type RunRedoLogGCResponse struct {
	Ack bool
}

type RunForcedAbortGCRequest struct{}

type RunForcedAbortGCResponse struct {
	Ack bool
}

type BackupLockAllRequest struct {
	Owner string
}

type BackupLockAllResponse struct {
	Ack bool
}

type BackupUnlockAllRequest struct {
	Owner string
}

type BackupUnlockAllResponse struct {
	Ack bool
}

type BackupRenewLockRequest struct {
	Owner string
}

type BackupRenewLockResponse struct {
	Ack bool
}

type BeginBackupPhaseRequest struct {
	BackupID string
}

type BeginBackupPhaseResponse struct {
	Ack         bool
	BoundaryTID TransactionID
}

type CompleteBackupPhaseRequest struct {
	BackupID string
}

type CompleteBackupPhaseResponse struct {
	Ack            bool
	SnapshotPath   string
	SnapshotSHA256 string
}

// Ideas for additional recovery and notification RPCs:
// - RecoverVoteQuery(tid)
// - AppliedTidsNotify(fromMemID, tids[])
