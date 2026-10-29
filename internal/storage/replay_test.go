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

func TestReplayStoreBytesReturnsDurableSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 11, Counter: 1, Epoch: 7}
	resp := sinfonia.ExecPrepareResponse{
		RequestID:    "prepare-1",
		Vote:         sinfonia.VoteOk,
		ReadData:     map[uint32][]byte{1: []byte("alpha")},
		CurrentEpoch: 13,
	}
	if err := store.AppendPrepare(txId, resp); err != nil {
		t.Fatal(err)
	}

	got, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(dir, "replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot bytes mismatch:\n got %q\nwant %q", string(got), string(want))
	}
}

func TestReplayStoreReplaceFromPublishesAndRefreshesIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	oldTID := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	if err := store.AppendPrepare(oldTID, sinfonia.ExecPrepareResponse{RequestID: "old", Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{1: []byte("old")}}); err != nil {
		t.Fatal(err)
	}

	expectedPrepareTID := sinfonia.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2}
	expectedPrepare := sinfonia.ExecPrepareResponse{
		RequestID:    "prepare-2",
		Vote:         sinfonia.VoteBadCmp,
		ReadData:     map[uint32][]byte{2: []byte("bravo")},
		CurrentEpoch: 21,
	}
	expectedOnePCTID := sinfonia.TransactionID{CoordinatorID: 3, Counter: 3, Epoch: 3}
	expectedOnePC := sinfonia.ExecCommit1PCResponse{
		RequestID:    "1pc-3",
		Vote:         sinfonia.VoteBadLock,
		ReadData:     map[uint32][]byte{3: []byte("charlie")},
		CurrentEpoch: 34,
		LockConflict: &sinfonia.LockConflict{Reason: "held", RequestedStart: 4, RequestedEnd: 9, HeldStart: 10, HeldEnd: 12, HeldWrite: true},
	}
	stagedPath, stagedBytes := writeReplayArtifact(t, func(staged *storage.ReplayStore) error {
		if err := staged.AppendPrepare(expectedPrepareTID, expectedPrepare); err != nil {
			return err
		}
		return staged.AppendOnePC(expectedOnePCTID, expectedOnePC)
	})

	if err := store.ReplaceFrom(stagedPath); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetPrepare(oldTID); ok {
		t.Fatal("old replay record remained after replace")
	}
	if got, ok := store.GetPrepare(expectedPrepareTID); !ok || !reflect.DeepEqual(got, expectedPrepare) {
		t.Fatalf("prepare replay = %+v ok=%v, want %+v", got, ok, expectedPrepare)
	}
	if got, ok := store.GetOnePC(expectedOnePCTID); !ok || !reflect.DeepEqual(got, expectedOnePC) {
		t.Fatalf("1pc replay = %+v ok=%v, want %+v", got, ok, expectedOnePC)
	}

	gotBytes, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBytes, stagedBytes) {
		t.Fatalf("published replay bytes mismatch:\n got %q\nwant %q", string(gotBytes), string(stagedBytes))
	}

	reopened, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopened.GetPrepare(expectedPrepareTID); !ok || !reflect.DeepEqual(got, expectedPrepare) {
		t.Fatalf("reopened prepare replay = %+v ok=%v, want %+v", got, ok, expectedPrepare)
	}
	if got, ok := reopened.GetOnePC(expectedOnePCTID); !ok || !reflect.DeepEqual(got, expectedOnePC) {
		t.Fatalf("reopened 1pc replay = %+v ok=%v, want %+v", got, ok, expectedOnePC)
	}
}

func TestReplayStoreReplaceFromRejectsIncompleteJSONL(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	oldTID := sinfonia.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: 4}
	oldResp := sinfonia.ExecPrepareResponse{RequestID: "old", Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{9: []byte("stable")}}
	if err := store.AppendPrepare(oldTID, oldResp); err != nil {
		t.Fatal(err)
	}
	before, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	validPath, validBytes := writeReplayArtifact(t, func(staged *storage.ReplayStore) error {
		return staged.AppendPrepare(
			sinfonia.TransactionID{CoordinatorID: 10, Counter: 2, Epoch: 5},
			sinfonia.ExecPrepareResponse{RequestID: "new", Vote: sinfonia.VoteBadForced, ReadData: map[uint32][]byte{10: []byte("new")}},
		)
	})
	if len(validBytes) < 2 {
		t.Fatalf("valid staged replay too small: %d", len(validBytes))
	}
	truncated := validBytes[:len(validBytes)-2]
	if err := os.WriteFile(validPath, truncated, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := store.ReplaceFrom(validPath); err == nil {
		t.Fatal("expected incomplete staged replay to be rejected")
	}

	after, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("failed replace changed durable replay:\n got %q\nwant %q", string(after), string(before))
	}
	if got, ok := store.GetPrepare(oldTID); !ok || !reflect.DeepEqual(got, oldResp) {
		t.Fatalf("failed replace changed in-memory replay: %+v ok=%v", got, ok)
	}

	reopened, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopened.GetPrepare(oldTID); !ok || !reflect.DeepEqual(got, oldResp) {
		t.Fatalf("failed replace changed reopened replay: %+v ok=%v", got, ok)
	}
}

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
	want, err := stored.Bytes()
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
	prefix, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	tailTID := sinfonia.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2}
	tailStore, err := storage.OpenReplayStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := tailStore.AppendPrepare(tailTID, sinfonia.ExecPrepareResponse{RequestID: "tail", Vote: sinfonia.VoteOk}); err != nil {
		t.Fatal(err)
	}
	tail, err := tailStore.Bytes()
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

func writeReplayArtifact(t *testing.T, fill func(*storage.ReplayStore) error) (string, []byte) {
	t.Helper()
	stagedDir := t.TempDir()
	store, err := storage.OpenReplayStore(stagedDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fill(store); err != nil {
		t.Fatal(err)
	}
	data, err := store.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "staged-replay.log")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}
