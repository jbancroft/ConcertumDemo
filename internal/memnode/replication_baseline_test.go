package memnode_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/storage"
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
			seed, err := memnode.NewDurableNode(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.ApplyWriteForTest(0, []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}

			tid := proto.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: 1}
			redo, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := redo.AppendDurable(storage.RedoEntry{TID: tid, Participants: []uint32{1}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("stale")}}}); err != nil {
				t.Fatal(err)
			}
			decided, err := storage.OpenDecidedStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := decided.Append(tid, true); err != nil {
				t.Fatal(err)
			}
			forced, err := storage.OpenForcedAbortStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := forced.Add(tid); err != nil {
				t.Fatal(err)
			}
			if err := storage.StoreProcessedPointer(dir, 1); err != nil {
				t.Fatal(err)
			}

			replica, err := memnode.NewDurableNode(1, 64, dir)
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
			if _, err := replica.InstallReplicationBaseline(ctx, proto.InstallReplicationBaselineRequest{
				BarrierOwner: owner,
				Image:        image,
				SHA256:       checksum[:],
				BoundaryTID:  proto.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: 1},
			}); err == nil {
				t.Fatal("expected injected baseline boundary failure")
			}
			replica.UnlockAllOwned(owner)
			if err := replica.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := memnode.NewDurableNode(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.Read(ctx, 0, 12)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "old\x00\x00\x00\x00\x00\x00\x00\x00\x00" {
				t.Fatalf("recovered image = %q, want old baseline", string(got))
			}
			if restarted.RedoEntryCountForTest() != 1 || !restarted.HasDecidedForTest(tid) || !restarted.HasForcedAbortForTest(tid) {
				t.Fatal("rollback did not restore the complete old transaction metadata")
			}
		})
	}
}

func TestReplicationBaselineReplayInstallCrashBoundaryRecoversOldState(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	seed, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.ApplyWriteForTest(0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	oldTID := proto.TransactionID{CoordinatorID: 31, Counter: 1, Epoch: 1}
	oldResponse := proto.ExecPrepareResponse{RequestID: "old", Vote: proto.VoteOK, ReadData: map[uint32][]byte{1: []byte("old")}}
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
	newTID := proto.TransactionID{CoordinatorID: 32, Counter: 2, Epoch: 1}
	newResponse := proto.ExecPrepareResponse{RequestID: "new", Vote: proto.VoteBadCmp, ReadData: map[uint32][]byte{1: []byte("new")}}
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

	replica, err := memnode.NewDurableNode(1, 64, dir)
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
	begin, err := replica.BeginReplicationBaseline(ctx, proto.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    "replay-boundary-session",
		TotalSize:    uint64(len(image)),
		SHA256:       imageChecksum[:],
		ReplaySize:   uint64(len(stagedReplayBytes)),
		ReplaySHA256: replayChecksum[:],
		BoundaryTID:  proto.TransactionID{CoordinatorID: 33, Counter: 3, Epoch: 1},
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	imageChunkChecksum := sha256.Sum256(image)
	if written, err := replica.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "replay-boundary-session", Artifact: "image", Offset: 0, SHA256: imageChunkChecksum[:], Data: image}); err != nil || !written.Ack {
		t.Fatalf("image write response = %+v, err = %v", written, err)
	}
	replayChunkChecksum := sha256.Sum256(stagedReplayBytes)
	if written, err := replica.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "replay-boundary-session", Artifact: "replay", Offset: 0, SHA256: replayChunkChecksum[:], Data: stagedReplayBytes}); err != nil || !written.Ack {
		t.Fatalf("replay write response = %+v, err = %v", written, err)
	}
	if committed, err := replica.CommitReplicationBaseline(ctx, proto.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: "replay-boundary-session"}); err == nil || committed.Ack {
		t.Fatalf("commit response = %+v, err = %v, want replay boundary failure", committed, err)
	}

	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Read(ctx, 0, 12)
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
			tid := seedOldBaseline(t, dir)
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
			restarted, err := memnode.NewDurableNode(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if err := restarted.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.Read(context.Background(), 0, 64)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]byte, 64)
			if testCase.newGen {
				copy(want, []byte("new-baseline"))
				if string(got) != string(want) {
					t.Fatalf("post-commit crash image = %q, want new baseline", string(got))
				}
				if restarted.RedoEntryCountForTest() != 0 || restarted.HasDecidedForTest(tid) || restarted.HasForcedAbortForTest(tid) {
					t.Fatal("post-commit crash retained old transaction metadata")
				}
				return
			}
			copy(want, []byte("old"))
			if string(got) != string(want) {
				t.Fatalf("pre-commit crash image = %q, want old baseline", string(got))
			}
			if restarted.RedoEntryCountForTest() != 1 || !restarted.HasDecidedForTest(tid) || !restarted.HasForcedAbortForTest(tid) {
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
	node, err := memnode.NewDurableNode(1, 64, dir)
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
	_, _ = node.InstallReplicationBaseline(context.Background(), proto.InstallReplicationBaselineRequest{BarrierOwner: owner, Image: image, SHA256: checksum[:], BoundaryTID: proto.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: 1}})
	t.Fatal("crash hook did not fire")
}

func seedOldBaseline(t *testing.T, dir string) proto.TransactionID {
	t.Helper()
	node, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.ApplyWriteForTest(0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 9, Counter: 1, Epoch: 1}
	redo, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := redo.AppendDurable(storage.RedoEntry{TID: tid, Participants: []uint32{1}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("stale")}}}); err != nil {
		t.Fatal(err)
	}
	decided, err := storage.OpenDecidedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := decided.Append(tid, true); err != nil {
		t.Fatal(err)
	}
	forced, err := storage.OpenForcedAbortStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := forced.Add(tid); err != nil {
		t.Fatal(err)
	}
	if err := storage.StoreProcessedPointer(dir, 1); err != nil {
		t.Fatal(err)
	}
	return tid
}

func TestReplicationBaselineChunkChecksumMismatchDoesNotTouchLiveState(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	node, err := memnode.NewDurableNode(1, 64, dir)
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
	begin, err := node.BeginReplicationBaseline(ctx, proto.BeginReplicationBaselineRequest{BarrierOwner: owner, SessionID: "checksum-session", TotalSize: uint64(len(expected)), SHA256: expectedSum[:], BoundaryTID: proto.TransactionID{Epoch: 1}})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	different := make([]byte, 64)
	copy(different, []byte("different"))
	chunkSum := sha256.Sum256(different)
	write, err := node.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: "checksum-session", Offset: 0, SHA256: chunkSum[:], Data: different})
	if err != nil || !write.Ack {
		t.Fatalf("write response = %+v, err = %v", write, err)
	}
	if _, err := node.CommitReplicationBaseline(ctx, proto.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: "checksum-session"}); err == nil {
		t.Fatal("expected whole-image checksum mismatch")
	}
	got, err := node.Read(ctx, 0, 64)
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
	node, err := memnode.NewDurableNode(1, 64, dir)
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
	begin, err := node.BeginReplicationBaseline(ctx, proto.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		TotalSize:    uint64(len(image)),
		SHA256:       imageChecksum[:],
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	chunkChecksum := sha256.Sum256(image)
	written, err := node.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		Offset:       0,
		SHA256:       chunkChecksum[:],
		Data:         image,
	})
	if err != nil || !written.Ack {
		t.Fatalf("write response = %+v, err = %v", written, err)
	}

	tid := proto.TransactionID{CoordinatorID: 71, Counter: 1, Epoch: 1}
	entry := proto.RedoEntryPayload{
		TID:          tid,
		Participants: []uint32{1},
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 32, Data: []byte("live")}},
	}
	if response, err := node.ReplicateRedoEntry(ctx, proto.ReplicateRedoEntryRequest{Entry: entry}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("redo during staged baseline = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, proto.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("decision during staged baseline = %+v, err = %v", response, err)
	}
	replayTID := proto.TransactionID{CoordinatorID: 71, Counter: 2, Epoch: 1}
	replay := proto.ExecCommit1PCResponse{RequestID: "original", Vote: proto.VoteBadCmp, ReadData: map[uint32][]byte{3: []byte("replayed")}}
	if response, err := node.ReplicateReplayRecord(ctx, proto.ReplicateReplayRecordRequest{TID: replayTID, Kind: proto.ReplayRecordOnePC, OnePC: &replay}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("replay during staged baseline = %+v, err = %v", response, err)
	}

	committed, err := node.CommitReplicationBaseline(ctx, proto.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
	if err != nil || !committed.Ack {
		t.Fatalf("commit response = %+v, err = %v", committed, err)
	}
	if response, err := node.ReplicateRedoEntry(ctx, proto.ReplicateRedoEntryRequest{Entry: entry}); err != nil || !response.Ack {
		t.Fatalf("redo after baseline commit = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateReplayRecord(ctx, proto.ReplicateReplayRecordRequest{TID: replayTID, Kind: proto.ReplayRecordOnePC, OnePC: &replay}); err != nil || !response.Ack {
		t.Fatalf("replay after baseline commit = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, proto.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("decision after baseline commit = %+v, err = %v", response, err)
	}
	node.UnlockAllOwned(owner)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Read(ctx, 0, 36)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 36)
	copy(want, []byte("baseline"))
	copy(want[32:], []byte("live"))
	if string(got) != string(want) {
		t.Fatalf("restarted handoff image = %q, want %q", got, want)
	}
	replayed, err := restarted.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{TID: replayTID, RequestID: "retry"})
	if err != nil || replayed.RequestID != "retry" || replayed.Vote != replay.Vote || string(replayed.ReadData[3]) != "replayed" {
		t.Fatalf("restarted replay = %+v, err = %v", replayed, err)
	}
}

func TestReplicationBaselineFailedInstallRejectsIngressUntilRestartRecovery(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "replica")
	node, err := memnode.NewDurableNode(1, 64, dir)
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
	begin, err := node.BeginReplicationBaseline(ctx, proto.BeginReplicationBaselineRequest{
		BarrierOwner: owner,
		SessionID:    sessionID,
		TotalSize:    uint64(len(image)),
		SHA256:       checksum[:],
	})
	if err != nil || !begin.Ack {
		t.Fatalf("begin response = %+v, err = %v", begin, err)
	}
	if written, err := node.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{
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
	if committed, err := node.CommitReplicationBaseline(ctx, proto.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID}); !errors.Is(err, injected) || committed.Ack {
		t.Fatalf("failed commit response = %+v, err = %v", committed, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".baseline-install")); err != nil {
		t.Fatalf("active rollback journal: %v", err)
	}

	tid := proto.TransactionID{CoordinatorID: 72, Counter: 1, Epoch: 1}
	entry := proto.RedoEntryPayload{TID: tid, Participants: []uint32{1}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 24, Data: []byte("live")}}}
	replayTID := proto.TransactionID{CoordinatorID: 72, Counter: 2, Epoch: 1}
	replay := proto.ExecCommit1PCResponse{RequestID: "original", Vote: proto.VoteBadCmp, ReadData: map[uint32][]byte{4: []byte("retry")}}
	if response, err := node.ReplicateRedoEntry(ctx, proto.ReplicateRedoEntryRequest{Entry: entry}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("redo with active rollback journal = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateCommitDecision(ctx, proto.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("decision with active rollback journal = %+v, err = %v", response, err)
	}
	if response, err := node.ReplicateReplayRecord(ctx, proto.ReplicateReplayRecordRequest{TID: replayTID, Kind: proto.ReplayRecordOnePC, OnePC: &replay}); !errors.Is(err, memnode.ErrReplicationBaselineInProgress) || response.Ack {
		t.Fatalf("replay with active rollback journal = %+v, err = %v", response, err)
	}

	node.UnlockAllOwned(owner)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	restarted.ConfigureAsReplica(1)
	if response, err := restarted.ReplicateRedoEntry(ctx, proto.ReplicateRedoEntryRequest{Entry: entry}); err != nil || !response.Ack {
		t.Fatalf("redo after restart recovery = %+v, err = %v", response, err)
	}
	if response, err := restarted.ReplicateReplayRecord(ctx, proto.ReplicateReplayRecordRequest{TID: replayTID, Kind: proto.ReplayRecordOnePC, OnePC: &replay}); err != nil || !response.Ack {
		t.Fatalf("replay after restart recovery = %+v, err = %v", response, err)
	}
	if response, err := restarted.ReplicateCommitDecision(ctx, proto.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); err != nil || !response.Ack {
		t.Fatalf("decision after restart recovery = %+v, err = %v", response, err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredRetry, err := memnode.NewDurableNode(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredRetry.Close()
	if err := recoveredRetry.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := recoveredRetry.Read(ctx, 24, 4)
	if err != nil || string(got) != "live" {
		t.Fatalf("restarted retried write = %q, err = %v", got, err)
	}
	replayed, err := recoveredRetry.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{TID: replayTID, RequestID: "after-restart"})
	if err != nil || replayed.RequestID != "after-restart" || replayed.Vote != replay.Vote || string(replayed.ReadData[4]) != "retry" {
		t.Fatalf("restarted retried replay = %+v, err = %v", replayed, err)
	}
}
