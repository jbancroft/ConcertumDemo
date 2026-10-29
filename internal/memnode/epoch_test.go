package memnode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestParticipantEpochSharedAndMonotonic(t *testing.T) {
	now := time.Unix(100*3600, 0)
	first := NewParticipant(64)
	defer first.Close()
	first.nowFn = func() time.Time { return now }
	if got := first.currentEpochLocked(); got != 100 {
		t.Fatalf("initial epoch=%d, want 100", got)
	}
	now = now.Add(3*time.Hour + 30*time.Minute)
	second := NewParticipant(64)
	defer second.Close()
	second.nowFn = func() time.Time { return now }
	for _, node := range []*Participant{first, second} {
		if got := node.currentEpochLocked(); got != 103 {
			t.Fatalf("shared epoch=%d, want 103", got)
		}
	}
	now = now.Add(-5 * time.Hour)
	for _, node := range []*Participant{first, second} {
		if got := node.currentEpochLocked(); got != 103 {
			t.Fatalf("clock rollback reduced epoch to %d", got)
		}
	}
}

func TestEpochFloorProtectsDiscardedHistory(t *testing.T) {
	for _, kind := range []string{"forced-abort", "replay"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			now := time.Unix(100*3600, 0)
			node.nowFn = func() time.Time { return now }
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
			req := sinfonia.ExecPrepareRequest{TID: tid, CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("X")}}}
			gc := node.RunForcedAbortGC
			hasRecord := func() bool { return node.forcedAborts.IsForcedAbort(tid) }
			if kind == "forced-abort" {
				if _, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid}); err != nil {
					t.Fatal(err)
				}
			} else {
				if resp, err := node.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteBadCmp {
					t.Fatalf("prepare=%+v, err=%v", resp, err)
				}
				gc = func() error { return node.Recover(ctx) }
				hasRecord = func() bool { _, ok := node.replay.GetPrepare(tid); return ok }
			}
			now = now.Add(3 * time.Hour)
			// Prevent publishing the floor. History must remain until it is durable.
			floorPath := filepath.Join(dir, "epoch.floor")
			if err := os.Mkdir(floorPath, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := gc(); err == nil {
				t.Fatal("GC succeeded without persisting the epoch floor")
			}
			if !hasRecord() {
				t.Fatal("failed GC discarded transaction history")
			}
			if err := os.Remove(floorPath); err != nil {
				t.Fatal(err)
			}
			if err := gc(); err != nil {
				t.Fatal(err)
			}
			if hasRecord() {
				t.Fatal("GC retained expired history")
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewDurableParticipant(1, 64, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			now = now.Add(-3 * time.Hour)
			restarted.nowFn = func() time.Time { return now }
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if resp, err := restarted.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch || resp.CurrentEpoch != 103 {
				t.Fatalf("retry after restart=%+v, err=%v", resp, err)
			}
			if resp, err := restarted.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid}); err != nil || resp.Vote != sinfonia.VoteBadStaleEpoch || resp.CurrentEpoch != 103 {
				t.Fatalf("1PC after restart=%+v, err=%v", resp, err)
			}
		})
	}
}

func TestParticipantRejectsCorruptEpochFloor(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "epoch.floor"), []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if node, err := NewDurableParticipant(1, 64, dir); err == nil {
		_ = node.Close()
		t.Fatal("started without a valid epoch floor")
	}
}
