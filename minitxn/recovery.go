package minitxn

import (
	"errors"
	"strconv"
)

// TransactionID identifies one attempt, including its UTC-hour retention epoch.
type TransactionID struct {
	CoordinatorID uint64
	Counter       uint64
	Epoch         uint64
}

func (t TransactionID) CorrelationID() string {
	return "txn-" + strconv.FormatUint(t.CoordinatorID, 10) + "-" + strconv.FormatUint(t.Epoch, 10) + "-" + strconv.FormatUint(t.Counter, 10)
}

// RecoveryHandle can be saved as JSON and resolved with a new client. Keep it
// intact: resolution must contact every participant in the original attempt.
type RecoveryHandle struct {
	TID          TransactionID
	Participants []uint32
	Reads        []ReadItem
}

var ErrOutcomeExpired = errors.New("sinfonia: transaction evidence expired")
var ErrReadResultsUnavailable = errors.New("sinfonia: committed transaction's original read results unavailable")
var ErrUnknownOutcome = errors.New("sinfonia: transaction outcome unknown; operation may have committed")

// UnknownOutcomeError carries the handle needed to resolve an uncertain write.
// It survives wrapping through the index and SQL layers via errors.As.
type UnknownOutcomeError struct {
	Handle *RecoveryHandle
	Cause  error
}

func (e *UnknownOutcomeError) Error() string {
	if e.Cause != nil {
		return ErrUnknownOutcome.Error() + ": " + e.Cause.Error()
	}
	return ErrUnknownOutcome.Error()
}
func (e *UnknownOutcomeError) Unwrap() error        { return e.Cause }
func (e *UnknownOutcomeError) Is(target error) bool { return target == ErrUnknownOutcome }

// OutcomeError preserves uncertainty and its recovery handle. A committed result
// can still have an error if its original read payload could not be recovered.
func (r ExecResult) OutcomeError() error {
	if r.UnknownOutcome || r.Status == StatusUnknown {
		return &UnknownOutcomeError{Handle: r.Recovery, Cause: r.Err}
	}
	return r.Err
}
