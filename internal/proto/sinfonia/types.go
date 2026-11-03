package sinfonia

import (
	"strconv"

	"github.com/jbancroft/ConcertumDemo/minitxn"
)

// TransactionID identifies one attempt, including its UTC-hour epoch.
type TransactionID struct {
	CoordinatorID uint64
	Counter       uint64
	Epoch         uint64
}

func (t TransactionID) CorrelationID() string {
	return "txn-" + strconv.FormatUint(t.CoordinatorID, 10) + "-" + strconv.FormatUint(t.Epoch, 10) + "-" + strconv.FormatUint(t.Counter, 10)
}

// CmpItem requires the bytes at (MemID, Addr) to match Data.
type CmpItem struct {
	MemID uint32
	Addr  uint64
	Data  []byte
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
type Vote = minitxn.Vote

const (
	VoteBadCmp        = minitxn.VoteBadCmp
	VoteBadForced     = minitxn.VoteBadForced
	VoteBadInternal   = minitxn.VoteBadInternal
	VoteBadLock       = minitxn.VoteBadLock
	VoteBadStaleEpoch = minitxn.VoteBadStaleEpoch
	VoteOk            = minitxn.VoteOk
	VoteUnknown       = minitxn.VoteUnknown
)
