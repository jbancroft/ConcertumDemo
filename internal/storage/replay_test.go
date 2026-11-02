package storage_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestReplayStoreRejectsConflictingDuplicate(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 12, Counter: 1, Epoch: 1}
	if err := store.AppendPrepare(txId, sinfonia.ExecPrepareResponse{RequestID: "first", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendPrepare(txId, sinfonia.ExecPrepareResponse{RequestID: "different", Vote: sinfonia.VoteBadCmp}); !errors.Is(err, storage.ErrReplayConflict) {
		t.Fatalf("conflicting append error = %v, want ErrReplayConflict", err)
	}
	got, ok := store.GetPrepare(txId)
	if !ok || got.RequestID != "first" || got.Vote != sinfonia.VoteOk {
		t.Fatalf("conflicting append changed original record: %+v ok=%v", got, ok)
	}
}

func TestReplayStoreOpenTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	stable := sinfonia.ExecPrepareResponse{RequestID: "stable", Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{1: []byte("keep")}}
	stableTID := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	tailTID := sinfonia.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: 1}
	stored, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.AppendPrepare(stableTID, stable); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.AppendPrepare(tailTID, stable); err != nil {
		t.Fatal(err)
	}
	complete, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	tail := complete[len(want):]
	for _, cut := range []int{1, len(tail) / 2, len(tail) - 1} {
		path := filepath.Join(dir, "replay.log")
		if err := os.WriteFile(path, append(bytes.Clone(want), tail[:cut]...), 0o644); err != nil {
			t.Fatal(err)
		}
		reopened, err := storage.OpenReplayStore(dir)
		if err != nil {
			t.Fatalf("open with %d-byte torn tail: %v", cut, err)
		}
		got, ok := reopened.GetPrepare(stableTID)
		if !ok || !reflect.DeepEqual(got, stable) {
			t.Fatalf("stable record lost or changed: %+v ok=%v", got, ok)
		}
		if _, ok := reopened.GetPrepare(tailTID); ok {
			t.Fatal("torn tail was treated as a valid record")
		}
		if disk, err := os.ReadFile(path); err != nil || !bytes.Equal(disk, want) {
			t.Fatalf("torn tail not physically truncated: %v", err)
		}
		if err := reopened.AppendPrepare(tailTID, stable); err != nil {
			t.Fatal(err)
		}
		reopened, err = storage.OpenReplayStore(dir)
		if err != nil {
			t.Fatalf("reopen after repairing and appending: %v", err)
		}
		if got, ok := reopened.GetPrepare(tailTID); !ok || !reflect.DeepEqual(got, stable) {
			t.Fatalf("reply appended after repair was lost: %+v, %t", got, ok)
		}
	}
}

func TestReplayStoreOpenFailsClosedOnCorruptRecords(t *testing.T) {
	dir := t.TempDir()
	stable := sinfonia.ExecPrepareResponse{RequestID: "stable", Vote: sinfonia.VoteOk}
	stored, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.AppendPrepare(sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}, stable); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "replay.log")
	complete, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Damage the payload without shortening the frame. A gob decoding error
	// must not be mistaken for an incomplete record that can be discarded.
	corrupt := bytes.Clone(complete)
	corrupt[len(corrupt)-1] = 0xff
	for _, data := range [][]byte{corrupt, []byte("{\"tid\":{},\"kind\":\"prepare\"}\n")} {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := storage.OpenReplayStore(dir); err == nil {
			t.Fatal("accepted a corrupt or obsolete replay record")
		}
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("failed open modified replay log: %v", err)
		}
	}
}

func TestReplayStoreCompactPreservesRetryableTID(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Retry horizon is 8, current epoch 10. A TID is expired (dropped) iff
	// Epoch + 8 <= 10, i.e. Epoch <= 2.
	wellExpired := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1} // 1+8=9 <= 10 -> dropped
	edgeExpired := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 2} // 2+8=10 <= 10 -> dropped (boundary)
	edgeKept := sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: 3}    // 3+8=11 > 10 -> kept (just inside)
	deepKept := sinfonia.TransactionID{CoordinatorID: 1, Counter: 4, Epoch: 9}    // 9+8=17 > 10 -> kept
	zeroEpoch := sinfonia.TransactionID{CoordinatorID: 1, Counter: 5, Epoch: 0}   // 0+8=8 <= 10 -> dropped
	if err := store.AppendPrepare(wellExpired, sinfonia.ExecPrepareResponse{RequestID: "w", Vote: sinfonia.VoteBadCmp}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendPrepare(edgeExpired, sinfonia.ExecPrepareResponse{RequestID: "e", Vote: sinfonia.VoteBadCmp}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendPrepare(edgeKept, sinfonia.ExecPrepareResponse{RequestID: "k", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendPrepare(deepKept, sinfonia.ExecPrepareResponse{RequestID: "d", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendOnePC(zeroEpoch, sinfonia.ExecCommit1PCResponse{RequestID: "zero", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(storage.ReplayRetention{CurrentEpoch: 2, RetryHorizon: 2}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetOnePC(zeroEpoch); ok {
		t.Fatal("epoch-zero reply retained at the expiration boundary")
	}
	if err := store.Compact(storage.ReplayRetention{CurrentEpoch: 10, RetryHorizon: 8}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetPrepare(wellExpired); ok {
		t.Fatal("well-expired TID retained after compaction")
	}
	if _, ok := store.GetPrepare(edgeExpired); ok {
		t.Fatal("edge-expired TID (epoch+horizon==current) retained after compaction")
	}
	if _, ok := store.GetPrepare(edgeKept); !ok {
		t.Fatal("edge-kept TID (epoch+horizon>current) dropped by compaction")
	}
	if _, ok := store.GetPrepare(deepKept); !ok {
		t.Fatal("deep-kept TID dropped by compaction")
	}
	if _, ok := store.GetOnePC(zeroEpoch); ok {
		t.Fatal("expired epoch-zero TID retained after compaction")
	}
	// Reopen and check that the saved log contains the expected records.
	reopened, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.GetPrepare(edgeExpired); ok {
		t.Fatal("edge-expired TID survived after restart")
	}
	if _, ok := reopened.GetPrepare(edgeKept); !ok {
		t.Fatal("edge-kept TID lost after restart")
	}
	if _, ok := reopened.GetOnePC(zeroEpoch); ok {
		t.Fatal("expired epoch-zero TID survived after restart")
	}
}

func TestReplayStoreCompactUnboundedRetentionKeepsAll(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 2}
	if err := store.AppendPrepare(txId, sinfonia.ExecPrepareResponse{RequestID: "keep", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(storage.ReplayRetention{CurrentEpoch: 1000, RetryHorizon: 0}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetPrepare(txId); !ok {
		t.Fatal("unbounded retention (RetryHorizon 0) dropped a record")
	}
}

func TestReplayStoreGobRepliesSurviveReopenAndCompaction(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	expired := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	if err := store.AppendPrepare(expired, sinfonia.ExecPrepareResponse{Vote: sinfonia.VoteBadCmp}); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 256<<10)
	for i := range data {
		data[i] = byte(i)
	}
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 3}
	want := sinfonia.ExecPrepareResponse{
		RequestID: "original", Vote: sinfonia.VoteOk, CurrentEpoch: 3,
		ReadData: map[uint32][]byte{0: data, 1: nil},
	}
	if err := store.AppendPrepare(tid, want); err != nil {
		t.Fatal(err)
	}
	store, err = storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A duplicate must still match the original after gob decoding.
	if err := store.AppendPrepare(tid, want); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(storage.ReplayRetention{CurrentEpoch: 3, RetryHorizon: 2}); err != nil {
		t.Fatal(err)
	}
	oneTID := sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: 3}
	// Preserve a present response even when every field has its zero value.
	if err := store.AppendOnePC(oneTID, sinfonia.ExecCommit1PCResponse{}); err != nil {
		t.Fatal(err)
	}
	store, err = storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := store.GetPrepare(tid); !ok || !reflect.DeepEqual(got, want) {
		t.Fatal("gob changed original binary read results")
	}
	if _, ok := store.GetPrepare(expired); ok {
		t.Fatal("compacted reply survived restart")
	}
	if got, ok := store.GetOnePC(oneTID); !ok || !reflect.DeepEqual(got, sinfonia.ExecCommit1PCResponse{}) {
		t.Fatalf("zero-valued one-phase reply lost: %+v, %t", got, ok)
	}
}

func TestReplayStoreCompactRetentionAvoidsEpochOverflow(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 3, Counter: 1, Epoch: ^uint64(0)}
	if err := store.AppendPrepare(txId, sinfonia.ExecPrepareResponse{RequestID: "max", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(storage.ReplayRetention{CurrentEpoch: ^uint64(0), RetryHorizon: 2}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetPrepare(txId); !ok {
		t.Fatal("overflow-safe retention incorrectly dropped a near-maximum epoch")
	}
}
