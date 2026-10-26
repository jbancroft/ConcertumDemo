package proto

// RequestID identifies one delivery attempt for a logical transaction.
type RequestID string

// DeadlineDecisionProtocolVersion identifies the protocol that durably records
// a coordinator-selected deadline decision and gives it precedence in recovery.
const DeadlineDecisionProtocolVersion uint16 = 1

type DescribeTransactionCapabilitiesRequest struct{}

type DescribeTransactionCapabilitiesResponse struct {
	DeadlineDecisionProtocolVersion uint16
}

// ExecPrepareRequest models the EXEC_PREPARE participant RPC.
type ExecPrepareRequest struct {
	TID           TransactionID
	RequestID     RequestID
	CorrelationID string
	Participants  []uint32
	HasWrites     bool
	// DecisionDeadlineNanos is the earliest nonzero CmpItem.ValidUntil in
	// the complete transaction. DeadlineDecisionProtocolVersion must be current
	// whenever this field is nonzero.
	DecisionDeadlineNanos           int64
	DeadlineDecisionProtocolVersion uint16
	CmpItems                        []CmpItem
	ReadItems                       []ReadItem
	WriteItems                      []WriteItem
	Epoch                           uint64
	Class                           string
	Generation                      uint64
}

// ExecPrepareResponse carries a vote and any read payload for this participant.
type ExecPrepareResponse struct {
	RequestID    RequestID
	Vote         Vote
	ReadData     map[uint32][]byte
	CurrentEpoch uint64
	LockConflict *LockConflict
}

// CommitRequest models the COMMIT participant RPC.
type CommitRequest struct {
	TID                             TransactionID
	RequestID                       RequestID
	CorrelationID                   string
	ActionCommit                    bool
	Generation                      uint64
	DeadlineDecisionProtocolVersion uint16
}

// CommitResponse acknowledges COMMIT receipt.
type CommitResponse struct {
	RequestID RequestID
	Ack       bool
}

// ExecCommit1PCRequest executes and commits atomically at a single participant.
type ExecCommit1PCRequest struct {
	TID                             TransactionID
	RequestID                       RequestID
	CorrelationID                   string
	CmpItems                        []CmpItem
	ReadItems                       []ReadItem
	WriteItems                      []WriteItem
	Epoch                           uint64
	Class                           string
	Generation                      uint64
	DeadlineDecisionProtocolVersion uint16
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

// ReplayRecordKind identifies the terminal participant response being
// replicated. Replay records are keyed by (TID, Kind), because a transaction
// may have both a prepare response and a one-phase response in separate
// protocol paths.
type ReplayRecordKind string

const (
	ReplayRecordPrepare ReplayRecordKind = "prepare"
	ReplayRecordOnePC   ReplayRecordKind = "one_pc"
)

// ReplicateReplayRecordRequest carries an exact terminal participant response
// to a synced replica. The replica stores the response before acknowledging,
// so promotion can replay the original vote and read payload without
// reevaluating the request.
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

// ReplicationFencingProtocolVersion identifies managers that understand the
// outcome-unknown fencing publication response. Durable memnodes require this
// version before accepting a new fencing generation.
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
	TID                   TransactionID
	Participants          []uint32
	DecisionDeadlineNanos int64
	WriteItems            []WriteItem
}

type ListRedoEntriesResponse struct {
	Entries []RedoEntryPayload
}

// ExportReplicationBaselineResponse is a complete durable image captured
// under the manager's global write barrier. BoundaryTID is diagnostic evidence
// for the snapshot point; the image itself is authoritative for the replica.
type ExportReplicationBaselineRequest struct {
	BarrierOwner string
}

type ExportReplicationBaselineResponse struct {
	Ack         bool
	Image       []byte
	SHA256      []byte
	BoundaryTID TransactionID
}

type InstallReplicationBaselineRequest struct {
	BarrierOwner string
	Image        []byte
	SHA256       []byte
	BoundaryTID  TransactionID
}

type InstallReplicationBaselineResponse struct {
	Ack         bool
	BoundaryTID TransactionID
}

type ExportReplicationBaselineChunkRequest struct {
	BarrierOwner string
	// Artifact is empty or "image" for the durable memory image. "replay"
	// selects the exact-response replay log in the same baseline session.
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
}

type BeginReplicationBaselineRequest struct {
	BarrierOwner string
	SessionID    string
	TotalSize    uint64
	SHA256       []byte
	ReplaySize   uint64
	ReplaySHA256 []byte
	BoundaryTID  TransactionID
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

// RecoverForceAbortRequest models a management-node recovery RPC.
type RecoverForceAbortRequest struct {
	TID                             TransactionID
	Epoch                           uint64
	CorrelationID                   string
	DeadlineDecisionProtocolVersion uint16
}

// RecoverForceAbortResponse reports the participant's effective vote.
type RecoverForceAbortResponse struct {
	Vote           Vote
	KnownDecision  bool
	DecisionCommit bool
}

// RecoverDecideRequest drives the final recovery decision.
type RecoverDecideRequest struct {
	TID                             TransactionID
	ActionCommit                    bool
	CorrelationID                   string
	DeadlineDecisionProtocolVersion uint16
}

// RecoverDecideResponse acknowledges decision application.
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

type UpdateEntryRequest struct {
	MemID              uint32
	Entry              DirectoryEntry
	ExpectedGeneration uint64
	// ExpectedRevisionSet distinguishes an exact CAS of bootstrap revision zero
	// from the legacy generation-only request.
	ExpectedRevision    uint64
	ExpectedRevisionSet bool
}

type UpdateEntryResponse struct {
	Ack               bool
	CurrentGeneration uint64
	Revision          uint64
	OutcomeUnknown    bool
	Degraded          bool
}

// Optional future RPC placeholders from SPEC.md.
// - RecoverVoteQuery(tid)
// - AppliedTidsNotify(fromMemID, tids[])
