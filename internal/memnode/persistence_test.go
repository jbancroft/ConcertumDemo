package memnode_test

import (
	"bytes"
	"context"
	"errors"
	"net"
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
	assertParticipantReady(t, n, false)

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
	assertParticipantReady(t, n, true)
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
	// Ordered by dependency: write sizes precede the combined memory size.
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
	assertParticipantReady(t, node, false)
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
	assertParticipantReady(t, node, true)
}

func TestUncertainPrepareAndOnePCRequireRestart(t *testing.T) {
	for _, multi := range []bool{false, true} {
		path := "one_pc"
		if multi {
			path = "prepare"
		}

		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node := newRecoveredFailureTestNode(t, 1, dir)
			injected := errors.New("acknowledgement lost after persistence")

			node.SetRedoFsyncHookForTest(func(f *os.File) error {
				if err := f.Sync(); err != nil {
					return err
				}
				return injected
			})

			other := memnode.NewParticipantWithId(2, 16)
			nodes := map[uint32]client.Participant{1: node, 2: other}
			writes := []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}
			if multi {
				writes = append(writes, sinfonia.WriteItem{MemID: 2, Addr: 0, Data: []byte("B")})
			}
			coord := client.NewCoordinator(nodes)
			defer coord.Close()
			result := coord.ExecuteAndCommit(ctx, nil, nil, writes)
			if !result.UnknownOutcome || result.Committed || result.Recovery == nil || result.TransportErr == nil {
				t.Fatalf("ambiguous persistence must survive automatic resolution as unknown: %+v", result)
			}
			if !errors.Is(result.TransportErr, injected) {
				t.Errorf("original failure was lost: %v", result.TransportErr)
			}
			tid := result.Recovery.TID
			assertParticipantFaulted(t, node, tid)
			if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("node applied unresolved write: %q, %v", got, err)
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

			resolver := client.NewCoordinator(map[uint32]client.Participant{1: restarted, 2: other})
			defer resolver.Close()
			resolved := resolver.Resolve(ctx, *result.Recovery)
			if resolved.UnknownOutcome || !resolved.Committed || resolved.TransportErr != nil {
				t.Fatalf("same attempt did not resolve after restart: %+v", resolved)
			}
		})

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
	defer coord.Close()
	result := coord.ExecuteAndCommit(ctx, nil, nil, []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}})
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
	resolver := client.NewCoordinator(map[uint32]client.Participant{1: restarted})
	defer resolver.Close()
	resolved := resolver.Resolve(ctx, *result.Recovery)
	if resolved.UnknownOutcome || resolved.Committed || resolved.TransportErr != nil {
		t.Fatalf("missing redo must resolve to abort after reopening: %+v", resolved)
	}
	if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("unlogged transaction was applied: %q, %v", got, err)
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
	assertParticipantReady(t, node, false)
	pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 1 || pending.Entries[0].TID != tid {
		t.Fatalf("fault discarded pending transaction: %+v, %v", pending, err)
	}
	if node.ActiveLockCountForTest() == 0 {
		t.Fatal("fault released transaction locks")
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
}
