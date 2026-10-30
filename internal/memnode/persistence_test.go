package memnode_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Durable transaction records, write boundaries, and uncertain persistence failures.

func TestReadOnlyMinitransactionNotLogged(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	n, err := memnode.NewDurableParticipant(1, 32, filepath.Join(dir, "n"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 9, Counter: 9, Epoch: sinfonia.EpochAt(time.Now())}
	resp, err := n.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != sinfonia.VoteOk {
		t.Fatalf("expected OK vote, got %v", resp.Vote)
	}
	if n.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected no redo log entries for read-only transaction")
	}
}

func TestDurableOnePCWriteApplyFailureRecoversFromRedo(t *testing.T) {
	ctx := context.Background()
	n, err := memnode.NewDurableParticipant(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	fault := errors.New("injected write apply failure")
	n.SetWriteApplyFaultForTest(func(uint64, []byte) error { return fault })
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: sinfonia.EpochAt(time.Now())}
	resp, err := n.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		TID:        txId,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 4, Data: []byte("redo")}},
	})
	if !errors.Is(err, fault) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("ExecCommit1PC() = (%+v, %v), want injected failure", resp, err)
	}
	if got := n.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("redo entry count after failed apply = %d, want 1", got)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = n.RPCServer().Serve(listener) }()
	client := rpcwire.NewClient(listener.Addr().String(), rpcwire.ClientOptions{DialTimeout: time.Second})
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var read sinfonia.ExecCommit1PCResponse
	err = client.Call(readCtx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{ReadItems: []sinfonia.ReadItem{{Addr: 4, Len: 4}}}, &read)
	if err == nil || !strings.Contains(err.Error(), memnode.ErrRecoveryPending.Error()) {
		t.Fatalf("RPC Read while apply is failed = %v, want ErrRecoveryPending", err)
	}
	readyServer := httptest.NewServer(n.HTTPHandler(true))
	t.Cleanup(readyServer.Close)
	readyResp, err := http.Get(readyServer.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	if readyResp.StatusCode != http.StatusServiceUnavailable {
		readyResp.Body.Close()
		t.Fatalf("ready status while apply is failed = %d, want %d", readyResp.StatusCode, http.StatusServiceUnavailable)
	}
	readyResp.Body.Close()

	n.SetWriteApplyFaultForTest(nil)
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := n.ReadBytesForTest(4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "redo" {
		t.Fatalf("recovered value = %q, want redo", got)
	}
	retry, err := n.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		RequestID:  "retry-after-recovery",
		TID:        txId,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 4, Data: []byte("redo")}},
	})
	if !errors.Is(err, memnode.ErrReplayUnavailable) || retry.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("exact-TID retry = (%+v, %v), want replay-unavailable failure", retry, err)
	}
	got, err = n.ReadBytesForTest(4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "redo" {
		t.Fatalf("exact-TID retry changed value = %q, want redo", got)
	}
	readyResp, err = http.Get(readyServer.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	if readyResp.StatusCode != http.StatusOK {
		readyResp.Body.Close()
		t.Fatalf("ready status after recovery = %d, want %d", readyResp.StatusCode, http.StatusOK)
	}
	readyResp.Body.Close()
	if got := n.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("redo entry count after recovery = %d, want 1", got)
	}

	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = n.ReadBytesForTest(4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "redo" {
		t.Fatalf("value after idempotent recovery retry = %q, want redo", got)
	}
	if got := n.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("redo entry count after idempotent recovery retry = %d, want 1", got)
	}
}

func TestOnePCOutOfBoundsWriteDoesNotPersistSuccess(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 32, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	txId := sinfonia.TransactionID{CoordinatorID: 77, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	resp, err := n.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "invalid", WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 31, Data: []byte("XX")}}})
	if !errors.Is(err, memnode.ErrOutOfBounds) || resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("out-of-bounds response = %+v, err = %v", resp, err)
	}
	if n.RedoEntryCountForTest() != 0 || n.HasDecidedForTest(txId) {
		t.Fatal("out-of-bounds request created durable transaction state")
	}
	store, err := storage.OpenReplayStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if replay, ok := store.GetOnePC(txId); ok {
		t.Fatalf("out-of-bounds request persisted success replay: %+v", replay)
	}
}

func TestDurableLargeWritesSurviveRestart(t *testing.T) {
	const (
		firstWriteSize  = 64 << 10
		secondWriteSize = 256 << 10
		memorySize      = firstWriteSize + secondWriteSize
	)
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied", true: "committed before apply"}[interrupted], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := memnode.NewDurableParticipant(1, memorySize, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			want := make([]byte, memorySize)
			for i := range want {
				// Exercise every byte value, including invalid UTF-8 and newlines.
				want[i] = byte(i)
			}
			fault := errors.New("interrupted after saving decision")
			for i, write := range []sinfonia.WriteItem{
				{MemID: 1, Addr: 0, Data: want[:firstWriteSize]},
				{MemID: 1, Addr: firstWriteSize, Data: want[firstWriteSize:]},
			} {
				if interrupted && i == 1 {
					node.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
						if phase == "one-pc-before-response" {
							return fault
						}
						return nil
					})
				}
				resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
					TID:        sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: sinfonia.EpochAt(time.Now())},
					WriteItems: []sinfonia.WriteItem{write},
				})
				if interrupted && i == 1 {
					if !errors.Is(err, fault) {
						t.Fatalf("commit interruption: %v", err)
					}
					got, err := node.ReadBytesForTest(firstWriteSize, secondWriteSize)
					if err != nil || !bytes.Equal(got, make([]byte, secondWriteSize)) {
						t.Fatalf("interrupted write applied before restart: %v", err)
					}
				} else if err != nil || resp.Vote != sinfonia.VoteOk {
					t.Fatalf("large write: %+v, %v", resp, err)
				}
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node, err = memnode.NewDurableParticipant(1, memorySize, dir)
			if err != nil {
				t.Fatalf("reopen node after large writes: %v", err)
			}
			defer node.Close()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := node.ReadBytesForTest(0, memorySize)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("restart did not preserve all binary write data: %v", err)
			}
		})
	}
}

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

// Apply the operation before losing its acknowledgement. A negative Ack is
// equally ambiguous: the primary cannot assume the replica rolled anything back.
type uncertainReplica struct {
	*memnode.Participant
	phase string
	err   error
}

func (r uncertainReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	resp, err := r.Participant.ReplicateRedoEntry(ctx, req)
	if err == nil && r.phase == "redo" {
		return sinfonia.ReplicateRedoEntryResponse{}, r.err
	}
	return resp, err
}

func (r uncertainReplica) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	resp, err := r.Participant.ReplicateCommitDecision(ctx, req)
	if err == nil && r.phase == "decision" {
		return sinfonia.ReplicateCommitDecisionResponse{}, r.err
	}
	return resp, err
}

func TestUncertainPrepareAndOnePCRequireRestart(t *testing.T) {
	for _, multi := range []bool{false, true} {
		path := "one_pc"
		if multi {
			path = "prepare"
		}
		for _, failure := range []string{"redo_sync", "replica_redo_error", "replica_redo_nack", "replica_decision_error", "replica_decision_nack"} {
			if multi && (failure == "replica_decision_error" || failure == "replica_decision_nack") {
				continue // Phase-two delivery is covered separately below.
			}
			t.Run(path+"/"+failure, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				node := newRecoveredFailureTestNode(t, 1, dir)
				injected := errors.New("acknowledgement lost after persistence")
				var replica *memnode.Participant
				if failure == "redo_sync" {
					node.SetRedoFsyncHookForTest(func(f *os.File) error {
						if err := f.Sync(); err != nil {
							return err
						}
						return injected
					})
				} else {
					replica = newRecoveredFailureTestNode(t, 3, t.TempDir())
					replica.ConfigureAsReplica(1)
					peer := uncertainReplica{Participant: replica, phase: "redo", err: injected}
					if failure == "replica_decision_error" || failure == "replica_decision_nack" {
						peer.phase = "decision"
					}
					if failure == "replica_redo_nack" || failure == "replica_decision_nack" {
						peer.err = nil
					}
					node.ConfigureAsPrimary(3, peer)
				}
				other := memnode.NewParticipantWithId(2, 16)
				nodes := map[uint32]client.Participant{1: node, 2: other}
				writes := []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}
				if multi {
					writes = append(writes, sinfonia.WriteItem{MemID: 2, Addr: 0, Data: []byte("B")})
				}
				coord := client.NewCoordinator(nodes)
				result := coord.ExecuteAndCommit(ctx, "", nil, nil, writes)
				if !result.UnknownOutcome || result.Committed || result.Recovery == nil || result.TransportErr == nil {
					t.Fatalf("ambiguous persistence must survive automatic resolution as unknown: %+v", result)
				}
				if (failure == "redo_sync" || failure == "replica_redo_error" || failure == "replica_decision_error") && !errors.Is(result.TransportErr, injected) {
					t.Errorf("original failure was lost: %v", result.TransportErr)
				}
				tid := result.Recovery.TID
				assertParticipantFaulted(t, node, tid)
				if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
					t.Fatalf("primary applied unresolved write: %q, %v", got, err)
				}
				if replica != nil && (failure == "replica_decision_error" || failure == "replica_decision_nack") {
					if got, err := replica.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
						t.Fatalf("test must lose the acknowledgement after replica apply: %q, %v", got, err)
					}
				}

				// Reopen the files: Recover on the old object would miss a redo
				// record whose sync returned an error after writing it to disk.
				if err := node.Close(); err != nil {
					t.Fatal(err)
				}
				restarted, err := memnode.NewDurableParticipant(1, 16, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restarted.Close() })
				restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: other})
				if err := restarted.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
					t.Fatalf("restart lost persisted write: %q, %v", got, err)
				}
				if replica != nil {
					// Keep application traffic stopped until the replica has a
					// fresh baseline, including decisions it may have missed.
					restarted.ConfigureAsPrimary(3, replica)
					if err := replica.SyncFromPrimary(ctx, restarted); err != nil {
						t.Fatal(err)
					}
					if got, err := replica.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
						t.Fatalf("resynchronization lost committed write: %q, %v", got, err)
					}
				}
				resolved := client.NewCoordinator(map[uint32]client.Participant{1: restarted, 2: other}).Resolve(ctx, *result.Recovery)
				if resolved.UnknownOutcome || !resolved.Committed || resolved.TransportErr != nil {
					t.Fatalf("same attempt did not resolve after restart: %+v", resolved)
				}
			})
		}
	}
}

func TestRedoFailureBeforeWriteResolvesAbortAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	// Make opening the log fail before writing any bytes.
	path := filepath.Join(dir, "redo.log")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: node})
	result := coord.ExecuteAndCommit(ctx, "", nil, nil, []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}})
	if !result.UnknownOutcome || result.Recovery == nil {
		t.Fatalf("expected conservative uncertainty: %+v", result)
	}
	assertParticipantFaulted(t, node, result.Recovery.TID)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoveredFailureTestNode(t, 1, dir)
	resolved := client.NewCoordinator(map[uint32]client.Participant{1: restarted}).Resolve(ctx, *result.Recovery)
	if resolved.UnknownOutcome || resolved.Committed || resolved.TransportErr != nil {
		t.Fatalf("missing redo must resolve to abort after reopening: %+v", resolved)
	}
	if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("unlogged transaction was applied: %q, %v", got, err)
	}
}

func TestUncertainTwoPhaseDecisionRequiresRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	replica := newRecoveredFailureTestNode(t, 2, t.TempDir())
	replica.ConfigureAsReplica(1)
	node.ConfigureAsPrimary(2, uncertainReplica{Participant: replica, phase: "decision", err: context.DeadlineExceeded})
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	prep, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}}})
	if err != nil || prep.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare failed: %+v, %v", prep, err)
	}
	committed, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true})
	if committed.Ack || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost decision acknowledgement must return an error: %+v, %v", committed, err)
	}
	assertParticipantFaulted(t, node, tid)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoveredFailureTestNode(t, 1, dir)
	if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
		t.Fatalf("saved decision lost its writes: %q, %v", got, err)
	}
}

func TestReplicaRedoFailureCannotBeBypassedByPromotion(t *testing.T) {
	ctx := context.Background()
	replica := newRecoveredFailureTestNode(t, 2, t.TempDir())
	replica.ConfigureAsReplica(1)
	replica.SetRedoFsyncHookForTest(func(f *os.File) error {
		if err := f.Sync(); err != nil {
			return err
		}
		return context.DeadlineExceeded
	})
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	entry := sinfonia.ReplicateRedoEntryRequest{Entry: sinfonia.RedoEntryPayload{TID: tid, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}}
	if resp, err := replica.ReplicateRedoEntry(ctx, entry); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("replica append did not fault: %+v, %v", resp, err)
	}
	if resp, err := replica.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted replica acknowledged a decision: %+v, %v", resp, err)
	}
	if resp, err := replica.ReplicateRedoEntry(ctx, entry); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted replica acknowledged another append: %+v, %v", resp, err)
	}
	replica.PromoteToPrimary(0, nil)
	status, err := replica.ReplicationStatus(ctx, sinfonia.ReplicationStatusRequest{})
	if err != nil || status.Ready {
		t.Fatalf("promotion cleared replica fault: %+v, %v", status, err)
	}
	if _, err := replica.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid}); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("promoted replica invented a vote: %v", err)
	}
	if err := replica.Recover(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("same-instance recovery cleared replica fault: %v", err)
	}
}

func TestInFlightReplicationStopsWhenAnotherTransactionFaults(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(map[bool]string{false: "one_pc", true: "prepare"}[prepare], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			node := newRecoveredFailureTestNode(t, 1, t.TempDir())
			peer := newBlockingReplica()
			node.ConfigureAsPrimary(2, peer)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			writes := []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}}
			done := make(chan error, 1)
			go func() {
				if prepare {
					_, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: writes})
					done <- err
				} else {
					_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: writes})
					done <- err
				}
			}()
			select {
			case <-peer.redoStarted:
			case <-ctx.Done():
				t.Fatal("request did not reach replication")
			}
			node.SetRedoFsyncHookForTest(func(*os.File) error { return context.DeadlineExceeded })
			other := tid
			other.Counter++
			_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: other, WriteItems: []sinfonia.WriteItem{{Addr: 8, Data: []byte("B")}}})
			close(peer.releaseRedo)
			if !errors.Is(err, memnode.ErrRestartRequired) {
				t.Fatalf("second request did not fault: %v", err)
			}
			if err := <-done; !errors.Is(err, memnode.ErrRestartRequired) {
				t.Fatalf("in-flight request ignored concurrent fault: %v", err)
			}
			pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
			if err != nil || len(pending.Entries) != 2 || node.StatsSnapshot().Locks.Active != 2 {
				t.Fatalf("concurrent fault discarded pending writes or locks: %+v, %v", pending, err)
			}
			if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("in-flight request applied after fault: %q, %v", got, err)
			}
		})
	}
}

func newRecoveredFailureTestNode(t *testing.T, id uint32, dir string) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewDurableParticipant(id, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if err := node.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return node
}

func assertParticipantFaulted(t *testing.T, node *memnode.Participant, tid sinfonia.TransactionID) {
	t.Helper()
	ctx := context.Background()
	ready := httptest.NewRecorder()
	node.HTTPHandler(false).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Errorf("faulted readiness = %d, want 503", ready.Code)
	}
	pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 1 || pending.Entries[0].TID != tid {
		t.Fatalf("fault discarded pending transaction: %+v, %v", pending, err)
	}
	stats := node.StatsSnapshot()
	if stats.Locks.Active == 0 {
		t.Fatalf("fault released transaction locks: %+v", stats.Locks)
	}
	unknown := sinfonia.TransactionID{CoordinatorID: 42, Epoch: sinfonia.EpochAt(time.Now())}
	for _, probe := range []sinfonia.TransactionID{tid, unknown} {
		if vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: probe}); err == nil {
			t.Errorf("faulted node invented a recovery vote: %+v", vote)
		}
		if status, err := node.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: probe}); err == nil {
			t.Errorf("faulted node reported a transaction outcome: %+v", status)
		}
		if _, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: probe}); err == nil {
			t.Error("faulted node accepted a one-phase request")
		}
		if _, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: probe}); err == nil {
			t.Error("faulted node accepted a prepare")
		}
	}
	for _, commit := range []bool{false, true} {
		if _, err := node.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: commit}); err == nil {
			t.Error("faulted node accepted a recovery decision")
		}
		if _, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: commit}); err == nil {
			t.Error("faulted node accepted a decision")
		}
	}
	if err := node.Recover(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Errorf("same-instance recovery cleared a fault without rereading storage: %v", err)
	}
	if _, err := node.ConfigureReplication(ctx, sinfonia.ConfigureReplicationRequest{Role: sinfonia.ReplicationRoleSingle}); err == nil {
		t.Error("role configuration bypassed the fault")
	}
	if err := node.LockAllBlockingOwned(ctx, "fault-test"); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Errorf("faulted node allowed a backup or baseline transfer: %v", err)
	}
}
