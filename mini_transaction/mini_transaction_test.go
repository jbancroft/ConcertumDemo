package mini_transaction

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"
)

type recordingExecutor struct {
	class    string
	compares []CompareItem
	reads    []ReadItem
	writes   []WriteItem
}

func (e *recordingExecutor) ExecuteAndCommit(_ context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
	e.class, e.compares, e.reads, e.writes = class, compares, reads, writes
	return ExecResult{Committed: true, Status: StatusCommitted}
}

func TestMiniTransactionRetainsOperationsAndOwnsData(t *testing.T) {
	executor := &recordingExecutor{}
	tx := NewMiniTransaction(executor).WithClass("gofonia")
	compareData, writeData := []byte("compare"), []byte("write")
	deadline := time.Date(2026, time.October, 27, 12, 0, 0, 0, time.UTC)
	// Separate calls catch gofonia's old value-receiver bug: operations must
	// stay in the builder even when the caller ignores the return value.
	tx.Compare(1, 4, compareData)
	tx.CompareValidUntil(2, 8, []byte("other"), deadline)
	tx.Read(2, 12, 4)
	tx.Read(3, 16, 8)
	tx.Write(1, 20, writeData)
	tx.Write(3, 24, []byte("third"))
	compareData[0], writeData[0] = 'X', 'X'

	compares := tx.CompareItemsForHost(1)
	if len(compares) != 1 || compares[0].Address != 4 || string(compares[0].Data) != "compare" {
		t.Fatalf("compare projection=%+v", compares)
	}
	compares[0].Data[0] = 'Y'
	tx.Compares()[0].Data[0] = 'Z'
	writes := tx.WriteItemsForHost(1)
	if len(writes) != 1 || writes[0].Address != 20 || string(writes[0].Data) != "write" {
		t.Fatalf("write projection=%+v", writes)
	}
	writes[0].Data[0] = 'Y'
	tx.Writes()[0].Data[0] = 'Z'
	reads := tx.ReadItemsForHost(2)
	if len(reads) != 1 || reads[0].Address != 12 || reads[0].Length != 4 {
		t.Fatalf("read projection=%+v", reads)
	}
	reads[0].Address = 99
	tx.Reads()[0].Length = 99
	if len(tx.CompareItemsForHost(99)) != 0 || len(tx.ReadItemsForHost(99)) != 0 || len(tx.WriteItemsForHost(99)) != 0 {
		t.Fatal("unknown host has transaction items")
	}
	hostIds := tx.UniqueHostIds()
	if !slices.Equal(hostIds, []uint32{1, 2, 3}) {
		t.Fatalf("host IDs=%v", hostIds)
	}
	hostIds[0] = 99
	if got := tx.UniqueHostIds(); !slices.Equal(got, []uint32{1, 2, 3}) {
		t.Fatalf("changing returned host IDs changed the transaction: %v", got)
	}

	committed, _, status := tx.ExecuteAndCommit(context.Background())
	if !committed || status != StatusCommitted || executor.class != "gofonia" {
		t.Fatalf("commit=%t status=%d class=%q", committed, status, executor.class)
	}
	if len(executor.compares) != 2 || len(executor.reads) != 2 || len(executor.writes) != 2 {
		t.Fatalf("executor lost operations: %+v", executor)
	}
	if string(executor.compares[0].Data) != "compare" || string(executor.writes[0].Data) != "write" || !executor.compares[1].ValidUntil.Equal(deadline) {
		t.Fatal("buffered data or deadline changed through a caller-owned slice")
	}
	if executor.reads[0].Address != 12 || executor.reads[0].Length != 4 {
		t.Fatal("read projection mutated the transaction")
	}
	executor.compares[0].Data[0] = 'Q'
	executor.writes[0].Data[0] = 'Q'
	if string(tx.Compares()[0].Data) != "compare" || string(tx.Writes()[0].Data) != "write" {
		t.Fatal("executor mutated the reusable transaction builder")
	}
}

func TestUniqueHostIdsPreservesUnsignedHostRange(t *testing.T) {
	tx := NewMiniTransaction(nil).
		Compare(math.MaxUint32, 0, []byte("old")).
		Read(0, 0, 1).
		Write(math.MaxUint32, 0, []byte("new"))
	if got := tx.UniqueHostIds(); !slices.Equal(got, []uint32{0, math.MaxUint32}) {
		t.Fatalf("host IDs=%v", got)
	}
}

func TestRangeItemsExposeByteRanges(t *testing.T) {
	for _, item := range []RangeItem{
		CompareItem{Address: 12, Data: []byte("abc")},
		ReadItem{Address: 12, Length: 3},
		WriteItem{Address: 12, Data: []byte("abc")},
	} {
		if item.Offset() != 12 || item.Size() != 3 {
			t.Fatalf("range=%d,%d", item.Offset(), item.Size())
		}
	}
}
