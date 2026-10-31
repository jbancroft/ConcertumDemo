// Package minitxn builds compare/read/write requests for the client.
// The builder and item names come from the earlier gofonia project.
package minitxn

// RangeItem gives the byte offset and size of a compare, read, or write.
type RangeItem interface {
	Offset() uint64
	Size() uint64
}

type CompareItem struct {
	HostId  uint32
	Address uint64
	Data    []byte
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

// Ordered by numeric value, with unknown as the zero value.
const (
	StatusUnknown Status = iota
	StatusCommitted
	StatusAborted
)

type ExecResult struct {
	Recovery         *RecoveryHandle
	Err              error
	Committed        bool
	ReadResults      map[ReadKey][]byte
	Status           Status
	ParticipantVotes map[uint32]int
	UnknownOutcome   bool
}
