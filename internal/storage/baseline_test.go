package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/storage"
)

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
	oldTID := proto.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	oldResp := proto.ExecPrepareResponse{RequestID: "old", Vote: proto.VoteOK, ReadData: map[uint32][]byte{1: []byte("old")}, CurrentEpoch: 9}
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
			proto.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2},
			proto.ExecPrepareResponse{RequestID: "new", Vote: proto.VoteBadCmp, ReadData: map[uint32][]byte{2: []byte("new")}, CurrentEpoch: 11},
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
	if _, ok := reopenedReplay.GetPrepare(proto.TransactionID{CoordinatorID: 2, Counter: 2, Epoch: 2}); ok {
		t.Fatal("recovered replay kept staged record")
	}
}
