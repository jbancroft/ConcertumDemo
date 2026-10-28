package proto

import (
	"time"

	"github.com/jbancroft/concertum2/mini_transaction"
)

// TransactionID combines a coordinator ID, counter, and epoch to identify an attempt.
type TransactionID struct {
	CoordinatorID uint64
	Counter       uint64
	Epoch         uint64
}

func (t TransactionID) CorrelationID() string {
	return "txn-" + itoa64(t.CoordinatorID) + "-" + itoa64(t.Epoch) + "-" + itoa64(t.Counter)
}

func itoa64(v uint64) string {
	if v == 0 {
		return "0"
	}
	const digits = "0123456789"
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}

// CmpItem requires the bytes at (MemID, Addr) to match Data. ValidUntil can also
// put a deadline on the transaction decision, including during recovery.
type CmpItem struct {
	MemID      uint32
	Addr       uint64
	Data       []byte
	ValidUntil time.Time
}

func (i CmpItem) Offset() uint64 { return i.Addr }
func (i CmpItem) Size() uint64   { return uint64(len(i.Data)) }

// ReadItem requests Len bytes at (MemID, Addr).
type ReadItem struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

func (i ReadItem) Offset() uint64 { return i.Addr }
func (i ReadItem) Size() uint64   { return uint64(i.Len) }

// WriteItem writes Data at (MemID, Addr).
type WriteItem struct {
	MemID uint32
	Addr  uint64
	Data  []byte
}

func (i WriteItem) Offset() uint64 { return i.Addr }
func (i WriteItem) Size() uint64   { return uint64(len(i.Data)) }

// Vote is a participant's answer to a prepare request.
type Vote = mini_transaction.Vote

const (
	VoteUnknown       = mini_transaction.VoteUnknown
	VoteOk            = mini_transaction.VoteOk
	VoteBadLock       = mini_transaction.VoteBadLock
	VoteBadForced     = mini_transaction.VoteBadForced
	VoteBadCmp        = mini_transaction.VoteBadCmp
	VoteBadStaleEpoch = mini_transaction.VoteBadStaleEpoch
	VoteBadInternal   = mini_transaction.VoteBadInternal
)
