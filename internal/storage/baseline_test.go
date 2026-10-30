package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Replication baseline replacement and recovery from interrupted installation.

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

func TestRecoverBaselineInstallFailsClosedOnIncompleteJournal(t *testing.T) {
	dir := t.TempDir()
	image, _, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := image.Replace([]byte("oldstate")); err != nil {
		t.Fatal(err)
	}
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.BeginBaselineInstall(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, ".baseline-install", "image.bin")); err != nil {
		t.Fatal(err)
	}
	if err := storage.RecoverBaselineInstall(dir); err == nil {
		t.Fatal("expected incomplete rollback journal to fail closed")
	}

	reopened, got, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if string(got) != "oldstate" {
		t.Fatalf("incomplete journal changed live image: got %q", string(got))
	}
}

func TestRecoverBaselineInstallRestoresReplayLogAndImage(t *testing.T) {
	dir := t.TempDir()
	image, _, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := image.Replace([]byte("oldstate")); err != nil {
		t.Fatal(err)
	}
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}

	replay, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	oldTID := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	oldResp := sinfonia.ExecPrepareResponse{RequestID: "old", Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{1: []byte("old")}, CurrentEpoch: 9}
	if err := replay.AppendPrepare(oldTID, oldResp); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.BeginBaselineInstall(dir); err != nil {
		t.Fatal(err)
	}

	replacementImage, _, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacementImage.Replace([]byte("newstate")); err != nil {
		t.Fatal(err)
	}
	if err := replacementImage.Close(); err != nil {
		t.Fatal(err)
	}

	stagedPath, _ := writeReplayArtifact(t, func(store *storage.ReplayStore) error {
		return store.AppendPrepare(
			sinfonia.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2},
			sinfonia.ExecPrepareResponse{RequestID: "new", Vote: sinfonia.VoteBadCmp, ReadData: map[uint32][]byte{2: []byte("new")}, CurrentEpoch: 11},
		)
	})
	if err := replay.ReplaceFrom(stagedPath); err != nil {
		t.Fatal(err)
	}

	if err := storage.RecoverBaselineInstall(dir); err != nil {
		t.Fatal(err)
	}

	reopenedImage, got, err := storage.OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedImage.Close()
	if string(got) != "oldstate" {
		t.Fatalf("recovered image = %q, want %q", string(got), "oldstate")
	}

	reopenedReplay, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopenedReplay.GetPrepare(oldTID); !ok || string(got.ReadData[1]) != "old" || got.RequestID != "old" {
		t.Fatalf("recovered replay missing old record: %+v ok=%v", got, ok)
	}
	if _, ok := reopenedReplay.GetPrepare(sinfonia.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2}); ok {
		t.Fatal("recovered replay kept staged record")
	}
}
