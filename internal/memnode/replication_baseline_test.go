package memnode_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestReplicationBaselineInstallCrashBoundariesRecoverOldState(t *testing.T) {
	phases := []string{
		"journal-created",
		"image-synced",
		"redo-reset",
		"decided-reset",
		"forced-abort-reset",
		"processed-pointer-reset",
		"before-commit",
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "replica")
			seed, err := memnode.NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.ApplyWriteForTest(0, []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}

			txId := sinfonia.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			redoLog, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer redoLog.Close()
			if err := redoLog.AddItem(storage.RedoLogItem{TransactionId: txId, HostIds: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("stale")}}}); err != nil {
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
			if err := storage.StoreProcessedPointer(dir, 1); err != nil {
				t.Fatal(err)
			}

			replica, err := memnode.NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := replica.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			replica.ConfigureAsReplica(1)
			const owner = "baseline-boundary-test"
			if err := replica.LockAllBlockingOwned(ctx, owner); err != nil {
				t.Fatal(err)
			}
			replica.SetReplicationBaselineBoundaryHookForTest(func(got string) error {
				if got == phase {
					return errors.New("injected baseline crash")
				}
				return nil
			})
			image := make([]byte, 64)
			copy(image, []byte("new-baseline"))
			checksum := sha256.Sum256(image)
			incomingFloor := sinfonia.EpochAt(time.Now()) + 3
			if _, err := replica.InstallReplicationBaseline(ctx, sinfonia.InstallReplicationBaselineRequest{
				BarrierOwner: owner,
				Image:        image,
				SHA256:       checksum[:],
				BoundaryTID:  sinfonia.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: sinfonia.EpochAt(time.Now())},
				EpochFloor:   incomingFloor,
			}); err == nil {
				t.Fatal("expected injected baseline boundary failure")
			}
			replica.UnlockAllOwned(owner)
			if err := replica.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := memnode.NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if floor, err := storage.LoadEpochFloor(dir); err != nil || floor != incomingFloor {
				t.Fatalf("baseline rollback lost epoch floor: floor=%d, err=%v", floor, err)
			}
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.ReadBytesForTest(0, 12)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "old\x00\x00\x00\x00\x00\x00\x00\x00\x00" {
				t.Fatalf("recovered image = %q, want old baseline", string(got))
			}
			if restarted.RedoEntryCountForTest() != 1 || !restarted.HasDecidedForTest(txId) || !restarted.HasForcedAbortForTest(txId) {
				t.Fatal("rollback did not restore the complete old transaction metadata")
			}
		})
	}
}

func TestReplicationBaselineTransfersEpochFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	primary, err := memnode.NewDurableParticipant(1, 64, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetClockForTest(func() time.Time { return time.Unix(103*3600, 0) })
	if err := primary.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	replica, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetClockForTest(func() time.Time { return time.Unix(100*3600, 0) })
	if err := replica.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replica)
	if err := replica.SyncFromPrimary(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetClockForTest(func() time.Time { return time.Unix(100*3600, 0) })
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := restarted.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		TID:        sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100},
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("X")}},
	})
	if err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch || resp.CurrentEpoch != 103 {
		t.Fatalf("replica lost primary's epoch floor: resp=%+v, err=%v", resp, err)
	}
}

func TestReplicationBaselineReplayInstallCrashBoundaryRecoversOldState(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	seed, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.ApplyWriteForTest(0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	oldTID := sinfonia.TransactionID{CoordinatorID: 31, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	oldResponse := sinfonia.ExecPrepareResponse{RequestID: "old", Vote: sinfonia.VoteOk, ReadData: map[uint32][]byte{1: []byte("old")}}
	oldReplay, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := oldReplay.AppendPrepare(oldTID, oldResponse); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	stagedDir := t.TempDir()
	stagedReplay, err := storage.OpenReplayStore(stagedDir)
	if err != nil {
		t.Fatal(err)
	}
	newTID := sinfonia.TransactionID{CoordinatorID: 32, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	newResponse := sinfonia.ExecPrepareResponse{RequestID: "new", Vote: sinfonia.VoteBadCmp, ReadData: map[uint32][]byte{1: []byte("new")}}
	if err := stagedReplay.AppendPrepare(newTID, newResponse); err != nil {
		t.Fatal(err)
	}
	stagedReplayBytes, err := stagedReplay.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	stagedReplayPath := filepath.Join(t.TempDir(), "replay.log")
	if err := os.WriteFile(stagedReplayPath, stagedReplayBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	replica, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.ConfigureAsReplica(1)
	const owner = "replay-boundary-test"
	if err := replica.LockAllBlockingOwned(ctx, owner); err != nil {
		t.Fatal(err)
	}
	defer replica.UnlockAllOwned(owner)
	replica.SetReplicationBaselineBoundaryHookForTest(func(phase string) error {
		if phase == "replay-synced" {
			return errors.New("injected replay baseline crash")
		}
		return nil
	})

	image := make([]byte, 64)
	copy(image, []byte("new-baseline"))
	imageChecksum := sha256.Sum256(image)
	replayChecksum := sha256.Sum256(stagedReplayBytes)
	begin, err := replica.BeginReplicationBaseline(ctx, sinfonia.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    "replay-boundary-session",
		TotalSize:    uint64(len(image)),
		SHA256:       imageChecksum[:],
		ReplaySize:   uint64(len(stagedReplayBytes)),
		ReplaySHA256: replayChecksum[:],
		BoundaryTID:  sinfonia.TransactionID{CoordinatorID: 33, Counter: 3, Epoch: sinfonia.EpochAt(time.Now())},
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	imageChunkChecksum := sha256.Sum256(image)
	if written, err := replica.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "replay-boundary-session", Artifact: "image", Offset: 0, SHA256: imageChunkChecksum[:], Data: image}); err != nil || !written.Ack {
		t.Fatalf("image write response = %+v, err = %v", written, err)
	}
	replayChunkChecksum := sha256.Sum256(stagedReplayBytes)
	if written, err := replica.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "replay-boundary-session", Artifact: "replay", Offset: 0, SHA256: replayChunkChecksum[:], Data: stagedReplayBytes}); err != nil || !written.Ack {
		t.Fatalf("replay write response = %+v, err = %v", written, err)
	}
	if committed, err := replica.CommitReplicationBaseline(ctx, sinfonia.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: "replay-boundary-session"}); err == nil || committed.Ack {
		t.Fatalf("commit response = %+v, err = %v, want replay boundary failure", committed, err)
	}

	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ReadBytesForTest(0, 12)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old\x00\x00\x00\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("recovered image = %q, want old baseline", string(got))
	}
	restartedReplay, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotResponse, ok := restartedReplay.GetPrepare(oldTID)
	if !ok || gotResponse.RequestID != oldResponse.RequestID || gotResponse.Vote != oldResponse.Vote {
		t.Fatalf("recovered old replay = %+v ok=%v", gotResponse, ok)
	}
	if _, ok := restartedReplay.GetPrepare(newTID); ok {
		t.Fatal("recovered replay retained staged record")
	}
}

func TestReplicationBaselineInstallCrashSubprocess(t *testing.T) {
	phases := []struct {
		name   string
		newGen bool
	}{
		{name: "journal-created"},
		{name: "image-synced"},
		{name: "redo-reset"},
		{name: "decided-reset"},
		{name: "forced-abort-reset"},
		{name: "processed-pointer-reset"},
		{name: "before-commit"},
		{name: "commit-published", newGen: true},
	}
	for _, testCase := range phases {
		t.Run(testCase.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "replica")
			txId := seedOldBaseline(t, dir)
			cmd := exec.Command(os.Args[0], "-test.run=^TestReplicationBaselineInstallCrashHelper$", "--")
			cmd.Env = append(os.Environ(),
				"CONCERTUM_BASELINE_CRASH_HELPER=1",
				"CONCERTUM_BASELINE_CRASH_DIR="+dir,
				"CONCERTUM_BASELINE_CRASH_PHASE="+testCase.name,
			)
			if err := cmd.Run(); err == nil {
				t.Fatal("crash helper exited successfully")
			} else {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 97 {
					t.Fatalf("crash helper error = %v, want exit 97", err)
				}
			}
			restarted, err := memnode.NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if err := restarted.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.ReadBytesForTest(0, 64)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]byte, 64)
			if testCase.newGen {
				copy(want, []byte("new-baseline"))
				if string(got) != string(want) {
					t.Fatalf("post-commit crash image = %q, want new baseline", string(got))
				}
				if restarted.RedoEntryCountForTest() != 0 || restarted.HasDecidedForTest(txId) || restarted.HasForcedAbortForTest(txId) {
					t.Fatal("post-commit crash retained old transaction metadata")
				}
				return
			}
			copy(want, []byte("old"))
			if string(got) != string(want) {
				t.Fatalf("pre-commit crash image = %q, want old baseline", string(got))
			}
			if restarted.RedoEntryCountForTest() != 1 || !restarted.HasDecidedForTest(txId) || !restarted.HasForcedAbortForTest(txId) {
				t.Fatal("pre-commit crash did not restore old transaction metadata")
			}
		})
	}
}

func TestReplicationBaselineInstallCrashHelper(t *testing.T) {
	if os.Getenv("CONCERTUM_BASELINE_CRASH_HELPER") != "1" {
		return
	}
	dir := os.Getenv("CONCERTUM_BASELINE_CRASH_DIR")
	phase := os.Getenv("CONCERTUM_BASELINE_CRASH_PHASE")
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	node.ConfigureAsReplica(1)
	const owner = "subprocess-crash"
	if err := node.LockAllBlockingOwned(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	node.SetReplicationBaselineBoundaryHookForTest(func(got string) error {
		if got == phase {
			os.Exit(97)
		}
		return nil
	})
	image := make([]byte, 64)
	copy(image, []byte("new-baseline"))
	checksum := sha256.Sum256(image)
	_, _ = node.InstallReplicationBaseline(context.Background(), sinfonia.InstallReplicationBaselineRequest{BarrierOwner: owner, Image: image, SHA256: checksum[:], BoundaryTID: sinfonia.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: sinfonia.EpochAt(time.Now())}})
	t.Fatal("crash hook did not fire")
}

func seedOldBaseline(t *testing.T, dir string) sinfonia.TransactionID {
	t.Helper()
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.ApplyWriteForTest(0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	redoLog, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer redoLog.Close()
	if err := redoLog.AddItem(storage.RedoLogItem{TransactionId: txId, HostIds: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("stale")}}}); err != nil {
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
	if err := storage.StoreProcessedPointer(dir, 1); err != nil {
		t.Fatal(err)
	}
	return txId
}

func TestReplicationBaselineChunkChecksumMismatchDoesNotTouchLiveState(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	node.ConfigureAsReplica(1)
	const owner = "checksum-test"
	if err := node.LockAllBlockingOwned(ctx, owner); err != nil {
		t.Fatal(err)
	}
	defer node.UnlockAllOwned(owner)
	expected := make([]byte, 64)
	copy(expected, []byte("expected"))
	expectedSum := sha256.Sum256(expected)
	begin, err := node.BeginReplicationBaseline(ctx, sinfonia.BeginReplicationBaselineRequest{BarrierOwner: owner, SessionID: "checksum-session", TotalSize: uint64(len(expected)), SHA256: expectedSum[:], BoundaryTID: sinfonia.TransactionID{Epoch: sinfonia.EpochAt(time.Now())}})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	different := make([]byte, 64)
	copy(different, []byte("different"))
	chunkSum := sha256.Sum256(different)
	write, err := node.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "checksum-session", Offset: 0, SHA256: chunkSum[:], Data: different})
	if err != nil || !write.Ack {
		t.Fatalf("write response = %+v, err = %v", write, err)
	}
	if _, err := node.CommitReplicationBaseline(ctx, sinfonia.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: "checksum-session"}); err == nil {
		t.Fatal("expected whole-image checksum mismatch")
	}
	got, err := node.ReadBytesForTest(0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(make([]byte, 64)) {
		t.Fatal("checksum mismatch changed the live replica image")
	}
}

func TestReplicationBaselineRejectsLiveIngressUntilCommit(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	node.ConfigureAsReplica(1)
	const owner = "redo-handoff-test"
	if err := node.LockAllBlockingOwned(ctx, owner); err != nil {
		t.Fatal(err)
	}

	image := make([]byte, 64)
	copy(image, []byte("baseline"))
	imageChecksum := sha256.Sum256(image)
	const sessionID = "redo-handoff-session"
	begin, err := node.BeginReplicationBaseline(ctx, sinfonia.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		TotalSize:    uint64(len(image)),
		SHA256:       imageChecksum[:],
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	chunkChecksum := sha256.Sum256(image)
	written, err := node.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		Offset:       0,
		SHA256:       chunkChecksum[:],
		Data:         image,
	})
	if err != nil || !written.Ack {
		t.Fatalf("write response = %+v, err = %v", written, err)
	}

	txId := sinfonia.TransactionID{CoordinatorID: 71, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	entry := sinfonia.RedoEntryPayload{
		TID:          txId,
		Participants: []uint32{1},
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 32, Data: []byte("live")}},
	}
	if response, err := node.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{Entry: entry}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("redo during staged baseline = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: txId, ActionCommit: true}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("decision during staged baseline = %+v, err = %v", response, err)
	}
	replayTID := sinfonia.TransactionID{CoordinatorID: 71, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	replay := sinfonia.ExecCommit1PCResponse{RequestID: "original", Vote: sinfonia.VoteBadCmp, ReadData: map[uint32][]byte{3: []byte("replayed")}}
	if response, err := node.ReplicateReplayRecord(ctx, sinfonia.ReplicateReplayRecordRequest{TID: replayTID, Kind: sinfonia.ReplayRecordOnePC, OnePC: &replay}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("replay during staged baseline = %+v, err = %v", response, err)
	}

	committed, err := node.CommitReplicationBaseline(ctx, sinfonia.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
	if err != nil || !committed.Ack {
		t.Fatalf("commit response = %+v, err = %v", committed, err)
	}
	if response, err := node.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{Entry: entry}); err != nil || !response.Ack {
		t.Fatalf("redo after baseline commit = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateReplayRecord(ctx, sinfonia.ReplicateReplayRecordRequest{TID: replayTID, Kind: sinfonia.ReplayRecordOnePC, OnePC: &replay}); err != nil || !response.Ack {
		t.Fatalf("replay after baseline commit = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: txId, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("decision after baseline commit = %+v, err = %v", response, err)
	}
	node.UnlockAllOwned(owner)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ReadBytesForTest(0, 36)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 36)
	copy(want, []byte("baseline"))
	copy(want[32:], []byte("live"))
	if string(got) != string(want) {
		t.Fatalf("restarted handoff image = %q, want %q", got, want)
	}
	replayed, err := restarted.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: replayTID, RequestID: "retry"})
	if err != nil || replayed.RequestID != "retry" || replayed.Vote != replay.Vote || string(replayed.ReadData[3]) != "replayed" {
		t.Fatalf("restarted replay = %+v, err = %v", replayed, err)
	}
}

func TestReplicationBaselineFailedInstallRejectsIngressUntilRestartRecovery(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	node.ConfigureAsReplica(1)
	const owner = "failed-handoff-test"
	if err := node.LockAllBlockingOwned(ctx, owner); err != nil {
		t.Fatal(err)
	}

	image := make([]byte, 64)
	copy(image, []byte("new-baseline"))
	checksum := sha256.Sum256(image)
	const sessionID = "failed-handoff-session"
	begin, err := node.BeginReplicationBaseline(ctx, sinfonia.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		TotalSize:    uint64(len(image)),
		SHA256:       checksum[:],
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	if written, err := node.WriteReplicationBaselineChunk(ctx, sinfonia.WriteReplicationBaselineChunkRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		Offset:       0,
		SHA256:       checksum[:],
		Data:         image,
	}); err != nil || !written.Ack {
		t.Fatalf("write response = %+v, err = %v", written, err)
	}
	injected := errors.New("injected install failure")
	node.SetReplicationBaselineBoundaryHookForTest(func(phase string) error {
		if phase == "image-synced" {
			return injected
		}
		return nil
	})
	if committed, err := node.CommitReplicationBaseline(ctx, sinfonia.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID}); !errors.Is(err, injected) || committed.Ack {
		t.Fatalf("failed commit response = %+v, err = %v", committed, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".baseline-install")); err != nil {
		t.Fatalf("active rollback journal: %v", err)
	}

	txId := sinfonia.TransactionID{CoordinatorID: 72, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	entry := sinfonia.RedoEntryPayload{TID: txId, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 24, Data: []byte("live")}}}
	replayTID := sinfonia.TransactionID{CoordinatorID: 72, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	replay := sinfonia.ExecCommit1PCResponse{RequestID: "original", Vote: sinfonia.VoteBadCmp, ReadData: map[uint32][]byte{4: []byte("retry")}}
	if response, err := node.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{Entry: entry}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("redo with active rollback journal = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: txId, ActionCommit: true}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("decision with active rollback journal = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateReplayRecord(ctx, sinfonia.ReplicateReplayRecordRequest{TID: replayTID, Kind: sinfonia.ReplayRecordOnePC, OnePC: &replay}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("replay with active rollback journal = %+v, err = %v", response, err)
	}

	node.UnlockAllOwned(owner)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	restarted.ConfigureAsReplica(1)
	if response, err := restarted.ReplicateRedoEntry(ctx, sinfonia.ReplicateRedoEntryRequest{Entry: entry}); err != nil || !response.Ack {
		t.Fatalf("redo after restart recovery = %+v, err = %v", response, err)
	}
	if response, err := restarted.ReplicateReplayRecord(ctx, sinfonia.ReplicateReplayRecordRequest{TID: replayTID, Kind: sinfonia.ReplayRecordOnePC, OnePC: &replay}); err != nil || !response.Ack {
		t.Fatalf("replay after restart recovery = %+v, err = %v", response, err)
	}
	if response, err := restarted.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: txId, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("decision after restart recovery = %+v, err = %v", response, err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredRetry, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredRetry.Close()
	if err := recoveredRetry.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := recoveredRetry.ReadBytesForTest(24, 4)
	if err != nil || string(got) != "live" {
		t.Fatalf("restarted retried write = %q, err = %v", got, err)
	}
	replayed, err := recoveredRetry.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: replayTID, RequestID: "after-restart"})
	if err != nil || replayed.RequestID != "after-restart" || replayed.Vote != replay.Vote || string(replayed.ReadData[4]) != "retry" {
		t.Fatalf("restarted retried replay = %+v, err = %v", replayed, err)
	}
}
