package sinfonia

import "github.com/jbancroft/ConcertumDemo/minitxn"

// TransactionID identifies a single attempt across client and recovery RPCs.
type TransactionID = minitxn.TransactionID

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
