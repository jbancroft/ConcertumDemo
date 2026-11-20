package sinfonia

// RequestID labels a request delivery attempt within a transaction.
type RequestID string

type ExecPrepareRequest struct {
	TID          TransactionID
	RequestID    RequestID
	Participants []uint32
	CmpItems     []CmpItem
	ReadItems    []ReadItem
	WriteItems   []WriteItem
	Epoch        uint64
}

type ExecPrepareResponse struct {
	RequestID    RequestID
	Vote         Vote
	ReadData     map[uint32][]byte
	CurrentEpoch uint64
	LockConflict *LockConflict
}

type CommitRequest struct {
	TID          TransactionID
	RequestID    RequestID
	ActionCommit bool
}

type CommitResponse struct {
	RequestID RequestID
	Ack       bool
}

type ExecCommit1PCRequest struct {
	TID        TransactionID
	RequestID  RequestID
	CmpItems   []CmpItem
	ReadItems  []ReadItem
	WriteItems []WriteItem
	Epoch      uint64
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

// RecoverForceAbortRequest asks a node to record an abort unless it already voted yes.
type RecoverForceAbortRequest struct {
	TID   TransactionID
	Epoch uint64
}

type RecoverForceAbortResponse struct {
	Vote           Vote
	KnownDecision  bool
	DecisionCommit bool
}

type RecoverDecideRequest struct {
	TID          TransactionID
	ActionCommit bool
}

type RecoverDecideResponse struct {
	Ack bool
}

type QueryInDoubtRequest struct{}

type InDoubtEntry struct {
	TID          TransactionID
	PreparedAt   int64
	Participants []uint32
}

type QueryInDoubtResponse struct {
	Entries []InDoubtEntry
}

type TidOutcome int32

// Ordered by wire value, with unknown as the zero value.
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
