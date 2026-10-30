package memnode_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
)

func TestOnePCDecisionFailureRetainsWritesAndLocksForRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// Fail only the decision append; the redo log must remain writable.
	decisionPath := filepath.Join(dir, "decided.log")
	if err := os.Remove(decisionPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(decisionPath, 0o755); err != nil {
		t.Fatal(err)
	}
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	response, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		TID: tid, ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}},
		WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}},
	})
	if err == nil || response.Vote != sinfonia.VoteBadInternal {
		t.Errorf("decision failure must return an error, not a definite abort: %+v, %v", response, err)
	}
	if node.RedoEntryCountForTest() != 1 {
		t.Fatal("failure occurred before the redo entry was saved")
	}
	ready := httptest.NewRecorder()
	node.HTTPHandler(true).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness after storage failure = %d, want 503", ready.Code)
	}
	if err := os.Remove(decisionPath); err != nil {
		t.Fatal(err)
	}

	// Even direct participant access must not overtake the unfinished write.
	probe := sinfonia.ExecCommit1PCRequest{
		TID:       sinfonia.TransactionID{CoordinatorID: 2, Counter: 1, Epoch: tid.Epoch},
		ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}},
	}
	blocked, err := node.ExecCommit1PC(ctx, probe)
	if err != nil || blocked.Vote != sinfonia.VoteBadLock {
		t.Errorf("pending write lost its lock: %+v, %v", blocked, err)
	}

	// Resolve without using the original response, as after a lost reply.
	peers := map[uint32]recovery.Participant{1: node}
	resolved := recovery.Resolve(ctx, tid, []uint32{1}, peers)
	if !resolved.Known || !resolved.Committed || resolved.Err != nil {
		t.Fatalf("resolution failed: %+v", resolved)
	}
	if got := resolved.Votes[1].ReadData[0]; string(got) != "\x00" {
		t.Errorf("resolution lost the pending original read: %q", got)
	}
	probe.TID.Counter++
	applied, err := node.ExecCommit1PC(ctx, probe)
	if err != nil || applied.Vote != sinfonia.VoteOk || string(applied.ReadData[0]) != "A" {
		t.Fatalf("recovery must apply the write and release its lock: %+v, %v", applied, err)
	}
	pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 0 {
		t.Fatalf("completed recovery left pending work: %+v, %v", pending, err)
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
	got, err := node.ReadBytesForTest(0, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("recovered write did not survive restart: %q, %v", got, err)
	}
	ready = httptest.NewRecorder()
	node.HTTPHandler(true).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("readiness after restart recovery = %d, want 200", ready.Code)
	}
}
