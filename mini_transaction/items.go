// Package mini_transaction contains the transaction vocabulary and builder
// adapted from gofonia. Execution is supplied by the coordinator-backed client.
package mini_transaction

import "time"

// RangeItem is the common byte-range view of compare, read, and write items.
// Addresses and sizes retain Concertum2's unsigned representation.
type RangeItem interface {
	Offset() uint64
	Size() uint64
}

type CompareItem struct {
	HostId     uint32
	Address    uint64
	Data       []byte
	ValidUntil time.Time
}

func (i CompareItem) Offset() uint64 { return i.Address }
func (i CompareItem) Size() uint64   { return uint64(len(i.Data)) }

type ReadItem struct {
	HostId  uint32
	Address uint64
	Length  uint32
}

func (i ReadItem) Offset() uint64 { return i.Address }
func (i ReadItem) Size() uint64   { return uint64(i.Length) }

type WriteItem struct {
	HostId  uint32
	Address uint64
	Data    []byte
}

func (i WriteItem) Offset() uint64 { return i.Address }
func (i WriteItem) Size() uint64   { return uint64(len(i.Data)) }

type ReadKey = ReadItem

type Status int

const (
	StatusUnknown Status = iota
	StatusCommitted
	StatusAborted
)

type ExecResult struct {
	Committed        bool
	ReadResults      map[ReadKey][]byte
	Status           Status
	ParticipantVotes map[uint32]int
	UnknownOutcome   bool
}
