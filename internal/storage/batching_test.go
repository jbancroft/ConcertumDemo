package storage_test

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestRedoLogBatchingReducesFsyncCallsAndPreservesDurability(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
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

	if got := fsyncCalls.Load(); got >= entries {
		t.Fatalf("expected batching to reduce fsync calls; got=%d entries=%d", got, entries)
	}

	reloaded, err := storage.OpenRedoLog(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(reloaded.Entries()); got != entries {
		t.Fatalf("expected %d durable entries, got %d", entries, got)
	}
}

func TestReplicationBaselineStoragePrimitivesResetDurableState(t *testing.T) {
	dir := t.TempDir()
	image, initial, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	replacement := []byte("baseline")
	if err := image.Replace(replacement); err != nil {
		t.Fatal(err)
	}
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	if string(initial) != string(make([]byte, 8)) {
		t.Fatalf("unexpected initial image: %q", string(initial))
	}
	reopened, got, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(replacement) {
		t.Fatalf("replacement image was not durable: got %q", string(got))
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: 1}
	redo, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := redo.AddItem(storage.RedoLogItem{TransactionId: txId}); err != nil {
		t.Fatal(err)
	}
	decided, err := storage.OpenDecidedTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := decided.AddItem(txId, true); err != nil {
		t.Fatal(err)
	}
	forced, err := storage.OpenForcedAbortLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := forced.AddItem(txId); err != nil {
		t.Fatal(err)
	}
	if err := redo.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := decided.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := forced.Reset(); err != nil {
		t.Fatal(err)
	}
	if len(redo.Entries()) != 0 {
		t.Fatal("redo reset retained entries")
	}
	if _, ok := decided.Get(txId); ok {
		t.Fatal("decided reset retained a transaction")
	}
	if forced.IsForcedAbort(txId) {
		t.Fatal("forced-abort reset retained a transaction")
	}
}
