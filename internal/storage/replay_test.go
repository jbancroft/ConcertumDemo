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
	stored, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.AppendPrepare(sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}, stable); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	// Add an unfinished line with no newline and invalid JSON. Loading must
	// discard it rather than invent a saved reply.
	f, err := os.OpenFile(filepath.Join(dir, "replay.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(`{"tid":{"coordinator_id":9,`)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatalf("open with torn tail failed: %v", err)
	}
	got, ok := reopened.GetPrepare(sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1})
	if !ok || !reflect.DeepEqual(got, stable) {
		t.Fatalf("stable record lost or changed: %+v ok=%v", got, ok)
	}
	if _, ok := reopened.GetPrepare(sinfonia.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: 1}); ok {
		t.Fatal("torn tail was treated as a valid record")
	}
	// The file should contain only the complete records.
	disk, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, want) {
		t.Fatalf("torn tail not physically truncated:\n got %q\nwant %q", string(disk), string(want))
	}
}

func TestReplayStoreOpenFailsClosedOnCompleteCorruptLine(t *testing.T) {
	dir := t.TempDir()
	stable := sinfonia.ExecPrepareResponse{RequestID: "stable", Vote: sinfonia.VoteOk}
	stored, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.AppendPrepare(sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}, stable); err != nil {
		t.Fatal(err)
	}
	// A bad record with a newline was fully written. Report corruption
	// instead of treating it as an unfinished tail.
	f, err := os.OpenFile(filepath.Join(dir, "replay.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("{this is not json}\n")); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenReplayStore(dir); err == nil {
		t.Fatal("expected open to fail closed on a complete corrupt line")
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

func TestReplayStoreOpenDiscardsValidUnterminatedTail(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	stableTID := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	if err := store.AppendPrepare(stableTID, sinfonia.ExecPrepareResponse{RequestID: "stable", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	prefix, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	tailTID := sinfonia.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2}
	tailDir := t.TempDir()
	tailStore, err := storage.OpenReplayStore(tailDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := tailStore.AppendPrepare(tailTID, sinfonia.ExecPrepareResponse{RequestID: "tail", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	tail, err := os.ReadFile(filepath.Join(tailDir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	tail = tail[:len(tail)-1]
	if err := os.WriteFile(filepath.Join(dir, "replay.log"), append(prefix, tail...), 0o644); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.GetPrepare(tailTID); ok {
		t.Fatal("valid unterminated tail was treated as a durable record")
	}
	disk, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, prefix) {
		t.Fatalf("valid unterminated tail not truncated: got %q want %q", disk, prefix)
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
