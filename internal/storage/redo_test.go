package storage_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Redo log reading, durable batching, and compaction.

func TestRedoLogLargeRecordsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	const (
		largerWriteSize = 256 << 10
		largeWriteSize  = 64 << 10
	)
	var want []storage.RedoLogItem
	for i, size := range []int{largeWriteSize, largerWriteSize, 1} {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j + i)
		}
		entry := storage.RedoLogItem{
			TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1},
			HostIds:       []uint32{1, 2},
			WriteItems:    []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: data}},
		}
		if err := log.AddItem(entry); err != nil {
			t.Fatal(err)
		}
		want = append(want, entry)
	}
	reopened, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatalf("reopen accepted large records: %v", err)
	}
	defer reopened.Close()
	if !reflect.DeepEqual(reopened.Entries(), want) {
		t.Fatal("reopened redo log changed record order, metadata, or binary payloads")
	}
}

func TestRedoLogReaderEOFAndMalformedRecords(t *testing.T) {
	const first = `{"tid":{"Counter":1},"participants":[1],"write_items":[]}`
	const second = `{"tid":{"Counter":2},"participants":[1],"write_items":[]}`
	for _, tc := range []struct {
		name    string
		data    string
		wantIDs []uint64
		wantErr bool
	}{
		{name: "empty"},
		{name: "whitespace", data: " \t\r\n\n "},
		{name: "multiple records and blank lines", data: first + "\r\n\n \t\n" + second + "\n", wantIDs: []uint64{1, 2}},
		{name: "complete final record without newline", data: first + "\n" + second, wantIDs: []uint64{1, 2}},
		{name: "malformed middle record", data: first + "\nnot JSON\n" + second + "\n", wantErr: true},
		{name: "truncated final record", data: first + "\n" + second[:len(second)-1], wantErr: true},
		{name: "invalid binary encoding", data: `{"write_items":[{"Data":"!!!"}]}` + "\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "redo.log")
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			log, err := storage.OpenRedoLog(dir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("accepted malformed redo log")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			var ids []uint64
			for _, entry := range log.Entries() {
				ids = append(ids, entry.TransactionId.Counter)
			}
			if !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Fatalf("record IDs = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}

func TestRedoLogBatchingReducesFsyncCallsAndPreservesDurability(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	log.ConfigureBatchingForTest(3*time.Millisecond, 64)

	var fsyncCalls atomic.Int64
	log.SetFsyncHookForTest(func(f *os.File) error {
		time.Sleep(2 * time.Millisecond)
		fsyncCalls.Add(1)
		return f.Sync()
	})

	const entries = 200
	var wg sync.WaitGroup
	wg.Add(entries)
	for i := 0; i < entries; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := log.AddItem(storage.RedoLogItem{TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1}, HostIds: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: []byte{byte(i)}}}})
			if err != nil {
				t.Errorf("append failed: %v", err)
			}
		}()
	}
	wg.Wait()
	for range 2 {
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if got := fsyncCalls.Load(); got >= entries {
		t.Fatalf("expected batching to reduce fsync calls; got=%d entries=%d", got, entries)
	}

	reloaded, err := storage.OpenRedoLog(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	if got := len(reloaded.Entries()); got != entries {
		t.Fatalf("expected %d durable entries, got %d", entries, got)
	}
}

func TestRedoLogCompactionReplacesCompleteFile(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(map[int]string{1: "partial", 2: "full"}[count], func(t *testing.T) {
			dir := t.TempDir()
			log, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			for i := uint64(1); i <= 2; i++ {
				if err := log.AddItem(storage.RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: i}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := storage.StoreProcessedPointer(dir, 2); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "redo.log")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer original.Close()
			if err := log.TruncateHead(count); err != nil {
				t.Fatal(err)
			}
			// Readers holding the old file see its complete contents, never
			// an empty or partly rewritten log while a replacement is built.
			if got, err := io.ReadAll(original); err != nil || !bytes.Equal(got, before) {
				t.Errorf("original log was rewritten in place: %q, %v", got, err)
			}
			if ptr, err := storage.LoadProcessedPointer(dir); err != nil || ptr != 0 {
				t.Errorf("checkpoint after compaction = %d, %v; want 0", ptr, err)
			}
			if err := log.AddItem(storage.RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 3}}); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			entries := reopened.Entries()
			if len(entries) != 3-count {
				t.Fatalf("reopened entries = %d, want %d", len(entries), 3-count)
			}
			for i, entry := range entries {
				if entry.TransactionId.Counter != uint64(count+i+1) {
					t.Fatalf("entry %d = %+v", i, entry)
				}
			}
		})
	}
}
