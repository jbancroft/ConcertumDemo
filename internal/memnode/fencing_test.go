package memnode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func fencingPublicationCheckpoints() []string {
	return []string{
		fencingCheckpointBeforeTempCreate,
		fencingCheckpointAfterTempCreate,
		fencingCheckpointAfterWrite,
		fencingCheckpointAfterTempSync,
		fencingCheckpointAfterPublish,
		fencingCheckpointAfterNamespaceSync,
		fencingCheckpointBeforeRetention,
		fencingCheckpointAfterRetentionRemove,
		fencingCheckpointAfterRetentionSync,
		fencingCheckpointAfterRetention,
		fencingCheckpointBeforeResponse,
	}
}

func fencingCheckpointPublishesGeneration(checkpoint string) bool {
	switch checkpoint {
	case fencingCheckpointAfterPublish, fencingCheckpointAfterNamespaceSync, fencingCheckpointBeforeRetention, fencingCheckpointAfterRetentionRemove, fencingCheckpointAfterRetentionSync, fencingCheckpointAfterRetention, fencingCheckpointBeforeResponse:
		return true
	default:
		return false
	}
}

func TestFencingGenerationRetentionKeepsLatestRecoverableStates(t *testing.T) {
	dir := t.TempDir()
	for generation := uint64(1); generation <= 5; generation++ {
		if err := persistFencingGeneration(dir, generation); err != nil {
			t.Fatalf("persist generation %d: %v", generation, err)
		}
	}
	files, err := fencingGenerationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != maxRetainedFencingGenerations {
		t.Fatalf("retained fencing generations = %d, want %d", len(files), maxRetainedFencingGenerations)
	}
	for _, want := range []uint64{3, 4, 5} {
		if _, err := os.Stat(fencingGenerationPath(dir, want)); err != nil {
			t.Fatalf("generation %d was not retained: %v", want, err)
		}
	}
	if generation, err := loadFencingGeneration(dir); err != nil || generation != 5 {
		t.Fatalf("loaded generation = %d, err=%v, want 5", generation, err)
	}
}

func TestDurableNodeBoundsExistingFencingGenerationHistoryOnStartup(t *testing.T) {
	dir := t.TempDir()
	for generation := uint64(1); generation <= 5; generation++ {
		if err := os.WriteFile(fencingGenerationPath(dir, generation), []byte(fmt.Sprintf("%d\n", generation)), 0o600); err != nil {
			t.Fatalf("seed generation %d: %v", generation, err)
		}
	}
	if files, err := fencingGenerationFiles(dir); err != nil || len(files) != 5 {
		t.Fatalf("seeded fencing generations = %d, err=%v, want 5", len(files), err)
	}
	node, err := NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	files, err := fencingGenerationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != maxRetainedFencingGenerations {
		t.Fatalf("startup retained fencing generations = %d, want %d", len(files), maxRetainedFencingGenerations)
	}
}

func TestFencingRetentionCheckpointFailureAfterCleanupIsOutcomeUnknown(t *testing.T) {
	dir := t.TempDir()
	for generation := uint64(1); generation <= 4; generation++ {
		if err := os.WriteFile(fencingGenerationPath(dir, generation), []byte(fmt.Sprintf("%d\n", generation)), 0o600); err != nil {
			t.Fatalf("seed generation %d: %v", generation, err)
		}
	}
	sentinel := errors.New("retention checkpoint failure")
	err := persistFencingGenerationWithHook(dir, 5, func(point string) error {
		if point == fencingCheckpointAfterRetention {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("persist error = %v, want %v", err, sentinel)
	}
	files, err := fencingGenerationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != maxRetainedFencingGenerations {
		t.Fatalf("retained fencing generations = %d, want %d", len(files), maxRetainedFencingGenerations)
	}
	if generation, err := loadFencingGeneration(dir); err != nil || generation != 5 {
		t.Fatalf("loaded generation = %d, err=%v, want 5", generation, err)
	}
}

func TestFencingCheckpointFailureNeverPublishesLowerGeneration(t *testing.T) {
	for _, checkpoint := range fencingPublicationCheckpoints() {
		t.Run(checkpoint, func(t *testing.T) {
			dir := t.TempDir()
			sentinel := errors.New("injected fencing publication failure")
			err := persistFencingGenerationWithHook(dir, 42, func(point string) error {
				if point == checkpoint {
					return sentinel
				}
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("persist error = %v, want %v", err, sentinel)
			}
			generation, loadErr := loadFencingGeneration(dir)
			if fencingCheckpointPublishesGeneration(checkpoint) {
				if loadErr != nil || generation != 42 {
					t.Fatalf("published generation = %d, err=%v, want durable 42", generation, loadErr)
				}
				return
			}
			if loadErr != nil || generation != 0 {
				t.Fatalf("pre-publication generation = %d, err=%v, want 0", generation, loadErr)
			}
		})
	}
}

func TestFencingPublicationFailurePreservesFencingAcrossRestart(t *testing.T) {
	ctx := context.Background()
	for _, checkpoint := range fencingPublicationCheckpoints() {
		t.Run(checkpoint, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "node")
			node, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Recover(ctx); err != nil {
				_ = node.Close()
				t.Fatal(err)
			}
			sentinel := errors.New("injected fencing publication failure")
			node.setFencingPublicationHookForTest(func(point string) error {
				if point == checkpoint {
					return sentinel
				}
				return nil
			})
			response, err := node.ConfigureReplication(ctx, sinfonia.ConfigureReplicationRequest{Role: sinfonia.ReplicationRoleSingle, Generation: 42})
			if fencingCheckpointPublishesGeneration(checkpoint) {
				if err != nil || response.Ack || !response.OutcomeUnknown || response.Generation != 42 {
					t.Fatalf("configure response=(%+v, %v), want explicit unknown generation 42", response, err)
				}
			} else if !errors.Is(err, sentinel) {
				t.Fatalf("configure error = %v, want %v", err, sentinel)
			}
			expectedGeneration := uint64(0)
			if fencingCheckpointPublishesGeneration(checkpoint) {
				expectedGeneration = 42
				if err := node.Write(ctx, 0, []byte("x")); !errors.Is(err, ErrStaleGeneration) {
					t.Fatalf("unfenced write error = %v, want %v", err, ErrStaleGeneration)
				}
			}
			status, err := node.ReplicationStatus(ctx, sinfonia.ReplicationStatusRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if status.Generation != expectedGeneration {
				t.Fatalf("live generation = %d, want %d", status.Generation, expectedGeneration)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.Recover(ctx); err != nil {
				_ = restarted.Close()
				t.Fatal(err)
			}
			defer restarted.Close()
			status, err = restarted.ReplicationStatus(ctx, sinfonia.ReplicationStatusRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if status.Generation != expectedGeneration {
				t.Fatalf("restart generation = %d, want %d", status.Generation, expectedGeneration)
			}
			if expectedGeneration != 0 {
				if err := restarted.Write(ctx, 0, []byte("x")); !errors.Is(err, ErrStaleGeneration) {
					t.Fatalf("restarted unfenced write error = %v, want %v", err, ErrStaleGeneration)
				}
				if _, err := restarted.ConfigureReplication(ctx, sinfonia.ConfigureReplicationRequest{Role: sinfonia.ReplicationRoleReplica, PartnerRPC: "127.0.0.1:1", Generation: expectedGeneration - 1}); !errors.Is(err, ErrStaleGeneration) {
					t.Fatalf("restarted stale control error = %v, want %v", err, ErrStaleGeneration)
				}
			}
		})
	}
}
