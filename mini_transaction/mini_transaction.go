package mini_transaction

import (
	"context"
	"time"
)

// Executor submits a complete compare/read/write request. The coordinator
// assigns a fresh transaction ID for each attempt, including retries.
type Executor interface {
	ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
}

// MiniTransaction buffers operations until ExecuteAndCommit. Its builder and
// host projections follow gofonia, using pointer receivers and owned byte slices.
type MiniTransaction struct {
	executor Executor
	class    string
	compares []CompareItem
	reads    []ReadItem
	writes   []WriteItem
}

func NewMiniTransaction(executor Executor) *MiniTransaction {
	return &MiniTransaction{executor: executor}
}

func (t *MiniTransaction) Compare(hostId uint32, address uint64, data []byte) *MiniTransaction {
	return t.CompareValidUntil(hostId, address, data, time.Time{})
}

// CompareValidUntil also requires a commit decision strictly before validUntil.
// A zero deadline places no time constraint on the comparison.
func (t *MiniTransaction) CompareValidUntil(hostId uint32, address uint64, data []byte, validUntil time.Time) *MiniTransaction {
	t.compares = append(t.compares, CompareItem{HostId: hostId, Address: address, Data: append([]byte(nil), data...), ValidUntil: validUntil})
	return t
}

func (t *MiniTransaction) Read(hostId uint32, address uint64, length uint32) *MiniTransaction {
	t.reads = append(t.reads, ReadItem{HostId: hostId, Address: address, Length: length})
	return t
}

func (t *MiniTransaction) Write(hostId uint32, address uint64, data []byte) *MiniTransaction {
	t.writes = append(t.writes, WriteItem{HostId: hostId, Address: address, Data: append([]byte(nil), data...)})
	return t
}

func (t *MiniTransaction) WithClass(class string) *MiniTransaction {
	t.class = class
	return t
}

func (t *MiniTransaction) Compares() []CompareItem {
	result := make([]CompareItem, 0, len(t.compares))
	for _, item := range t.compares {
		item.Data = append([]byte(nil), item.Data...)
		result = append(result, item)
	}
	return result
}

func (t *MiniTransaction) Reads() []ReadItem {
	return append([]ReadItem(nil), t.reads...)
}

func (t *MiniTransaction) Writes() []WriteItem {
	result := make([]WriteItem, 0, len(t.writes))
	for _, item := range t.writes {
		item.Data = append([]byte(nil), item.Data...)
		result = append(result, item)
	}
	return result
}

func (t *MiniTransaction) CompareItemsForHost(hostId uint32) []CompareItem {
	result := []CompareItem{}
	for _, item := range t.compares {
		if item.HostId == hostId {
			item.Data = append([]byte(nil), item.Data...)
			result = append(result, item)
		}
	}
	return result
}

func (t *MiniTransaction) ReadItemsForHost(hostId uint32) []ReadItem {
	result := []ReadItem{}
	for _, item := range t.reads {
		if item.HostId == hostId {
			result = append(result, item)
		}
	}
	return result
}

func (t *MiniTransaction) WriteItemsForHost(hostId uint32) []WriteItem {
	result := []WriteItem{}
	for _, item := range t.writes {
		if item.HostId == hostId {
			item.Data = append([]byte(nil), item.Data...)
			result = append(result, item)
		}
	}
	return result
}

// UniqueHostIds returns a detached set so callers cannot change the transaction
// by modifying the returned membership. uint32 host IDs convert losslessly.
func (t *MiniTransaction) UniqueHostIds() *Int64Set {
	uniqueHostIds := NewInt64Set()
	for _, item := range t.compares {
		uniqueHostIds.Add(int64(item.HostId))
	}
	for _, item := range t.reads {
		uniqueHostIds.Add(int64(item.HostId))
	}
	for _, item := range t.writes {
		uniqueHostIds.Add(int64(item.HostId))
	}
	return uniqueHostIds
}

func (t *MiniTransaction) ExecuteAndCommit(ctx context.Context) (bool, map[ReadKey][]byte, Status) {
	committed, reads, status, _ := t.ExecuteAndCommitDetailed(ctx)
	return committed, reads, status
}

func (t *MiniTransaction) ExecuteAndCommitDetailed(ctx context.Context) (bool, map[ReadKey][]byte, Status, map[uint32]int) {
	result := t.executor.ExecuteAndCommit(ctx, t.class, t.Compares(), t.Reads(), t.Writes())
	return result.Committed, result.ReadResults, result.Status, result.ParticipantVotes
}
