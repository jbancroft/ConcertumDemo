package memnode_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestRecoveryAfterGCWithProcessedCheckpoint(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "partial"}[partial], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			open := func() *memnode.Participant {
				n, err := memnode.NewDurableParticipant(1, 16, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = n.Close() })
				return n
			}
			node := open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			for i, value := range []byte("AB") {
				parts := []uint32{1}
				if partial && i == 1 {
					// The second entry must remain until the other participant
					// confirms application, even though its decision is known.
					parts = []uint32{1, 2}
				}
				tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: sinfonia.EpochAt(time.Now())}
				prepared, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
					TID: tid, Participants: parts,
					WriteItems: []sinfonia.WriteItem{{Addr: uint64(i), Data: []byte{value}}},
				})
				if err != nil || prepared.Vote != sinfonia.VoteOk {
					t.Fatalf("prepare: %+v, %v", prepared, err)
				}
				if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
					t.Fatalf("commit: %+v, %v", resp, err)
				}
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node = open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if ptr, err := storage.LoadProcessedPointer(dir); err != nil || ptr != 2 {
				t.Fatalf("checkpoint before GC = %d, %v; want 2", ptr, err)
			}
			if err := node.RunRedoLogGC(ctx); err != nil {
				t.Fatal(err)
			}
			wantRemaining := 0
			if partial {
				wantRemaining = 1
			}
			if got := node.RedoEntryCountForTest(); got != wantRemaining {
				t.Fatalf("remaining redo entries = %d, want %d", got, wantRemaining)
			}

			// Stop after the new commit decision is durable, before its write.
			interrupted := errors.New("interrupted before applying committed write")
			node.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
				if phase == "one-pc-before-response" {
					return interrupted
				}
				return nil
			})
			_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
				TID:        sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: sinfonia.EpochAt(time.Now())},
				WriteItems: []sinfonia.WriteItem{{Addr: 2, Data: []byte("C")}},
			})
			if !errors.Is(err, interrupted) {
				t.Fatalf("commit interruption: %v", err)
			}
			if got, err := node.ReadBytesForTest(2, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("write applied before restart: %q, %v", got, err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node = open()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if got, err := node.ReadBytesForTest(0, 3); err != nil || string(got) != "ABC" {
				t.Fatalf("recovered data = %q, %v; want ABC", got, err)
			}
		})
	}
}

func TestRedoLogGCRejectsConcurrentStartupRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("initial commit: %+v, %v", resp, err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	entered, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	node.SetRecoverBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
		if phase == "replay-before" {
			close(entered)
			select {
			case <-proceed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	done := make(chan struct{})
	var recoverErr error
	go func() { defer close(done); recoverErr = node.Recover(ctx) }()
	defer func() { release(); <-done }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := node.RunRedoLogGC(ctx); !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Errorf("GC during recovery = %v, want ErrRecoveryPending", err)
	}
	if got := node.RedoEntryCountForTest(); got != 1 {
		t.Errorf("GC changed recovery's log: %d entries", got)
	}
	release()
	<-done
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := node.RunRedoLogGC(ctx); err != nil {
		t.Fatalf("GC after recovery: %v", err)
	}
}

func TestRedoLogGCCheckpointFailurePreservesLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("initial commit: %+v, %v", resp, err)
	}
	// Block checkpoint publication without preventing writes to the redo log.
	if err := os.Mkdir(filepath.Join(dir, "processed.ptr"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := node.RunRedoLogGC(ctx); err == nil {
		t.Error("GC succeeded despite checkpoint failure")
	}
	if node.RedoEntryCountForTest() != 1 || !node.HasDecidedForTest(tid) {
		t.Error("failed GC discarded recovery evidence")
	}
	ready := httptest.NewRecorder()
	node.HTTPHandler(false).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness after failed GC = %d, want 503", ready.Code)
	}
	if err := os.Remove(filepath.Join(dir, "processed.ptr")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
		t.Fatalf("data after failed GC and restart = %q, %v", got, err)
	}
}
