package proto

import "time"

// TransactionID uniquely identifies a minitransaction and carries epoch information.
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

// CmpItem compares data at (MemID, Addr) with Data and may require the
// transaction decision to occur before ValidUntil, including during recovery.
type CmpItem struct {
	MemID      uint32
	Addr       uint64
	Data       []byte
	ValidUntil time.Time
}

// ReadItem requests Len bytes at (MemID, Addr).
type ReadItem struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

// WriteItem writes Data at (MemID, Addr).
type WriteItem struct {
	MemID uint32
	Addr  uint64
	Data  []byte
}

// Vote represents a participant's prepare-phase outcome.
type Vote int32

const (
	VoteOK Vote = iota
	VoteBadLock
	VoteBadForced
	VoteBadCmp
	VoteBadStaleEpoch
	VoteBadInternal
)
