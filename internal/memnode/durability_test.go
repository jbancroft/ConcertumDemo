package memnode_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestDurabilityCrashAfterPrepareBeforeCommitApply(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	aDir := filepath.Join(dir, "a")
	bDir := filepath.Join(dir, "b")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})

	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.ReadBytesForTest(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("expected recovered write to be applied, got %q", string(got))
	}
}

func TestRecoveryOutcomeDiscoveryFromPeerDecision(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	aDir := filepath.Join(dir, "a")
	bDir := filepath.Join(dir, "b")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 4, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 5, Data: []byte("K")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 5, Data: []byte("Z")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.ReadBytesForTest(5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) == "K" {
		t.Fatalf("expected write not applied when peer says abort")
	}
}

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

func TestRedoLogGCCommitAfterAllApplied(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	b, _ := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 11, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("a")}}})
	_, _ = b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("b")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	_, _ = b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected redo gc to truncate committed entry")
	}
	if a.HasDecidedForTest(txId) {
		t.Fatalf("expected decided entry cleaned")
	}
	if a.HasAppliedTrackingForTest(txId) {
		t.Fatalf("expected applied tracking cleaned")
	}
}

func TestRedoLogGCNoPrematureTruncationWithoutRemoteConfirm(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 12, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 1 {
		t.Fatalf("expected redo entry to remain without remote applied confirmation")
	}
}

func TestRedoLogGCAbortRemovedImmediately(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 13, Epoch: sinfonia.EpochAt(time.Now())}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 2, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false})

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected abort entry gc immediately")
	}
}

func TestForcedAbortEpochGC(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	n, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "n"))
	t.Cleanup(func() { _ = n.Close() })
	base := time.Unix(3600, 0)
	n.SetClockForTest(func() time.Time { return base })
	old := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(base)}
	recent := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 5}
	_, _ = n.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: old})
	_, _ = n.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: recent})
	base = base.Add(6 * time.Hour)

	if err := n.RunForcedAbortGC(); err != nil {
		t.Fatal(err)
	}
	if n.HasForcedAbortForTest(old) {
		t.Fatalf("expected old forced-abort removed")
	}
	if !n.HasForcedAbortForTest(recent) {
		t.Fatalf("expected recent forced-abort preserved")
	}
}

func TestRecoveryAfterPartialGC(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	bDir := filepath.Join(root, "b")
	a, _ := memnode.NewDurableParticipant(1, 64, aDir)
	b, _ := memnode.NewDurableParticipant(2, 64, bDir)
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	txId := sinfonia.TransactionID{CoordinatorID: 5, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	parts := []uint32{1, 2}
	_, _ = a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 7, Data: []byte("x")}}})
	_, _ = b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: parts, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 7, Data: []byte("y")}}})
	_, _ = a.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	_, _ = b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)
	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.ReadBytesForTest(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "x" {
		t.Fatalf("expected recovered value x, got %q", got)
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
