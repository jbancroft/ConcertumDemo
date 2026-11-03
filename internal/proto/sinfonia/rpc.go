package sinfonia

// RequestID labels a request delivery attempt within a transaction.
type RequestID string

// ExecPrepareRequest asks a participant to check and prepare its part of a transaction.
type ExecPrepareRequest struct {
	TID          TransactionID
	RequestID    RequestID
	Participants []uint32
	CmpItems     []CmpItem
	ReadItems    []ReadItem
	WriteItems   []WriteItem
	Epoch        uint64
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
	TID          TransactionID
	RequestID    RequestID
	ActionCommit bool
}

// CommitResponse acknowledges a commit or abort request.
type CommitResponse struct {
	RequestID RequestID
	Ack       bool
}

// ExecCommit1PCRequest combines execution and commit for a single participant.
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

// RecoverForceAbortResponse returns the participant's vote after that check.
type RecoverForceAbortResponse struct {
	Vote           Vote
	KnownDecision  bool
	DecisionCommit bool
}

// RecoverDecideRequest asks the node to apply a recovery decision.
type RecoverDecideRequest struct {
	TID          TransactionID
	ActionCommit bool
}

// RecoverDecideResponse acknowledges that recovery decision.
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
