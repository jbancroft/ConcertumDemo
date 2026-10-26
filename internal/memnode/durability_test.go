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

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/rpcwire"
)

func TestDurabilityCrashAfterPrepareBeforeCommitApply(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	aDir := filepath.Join(dir, "a")
	bDir := filepath.Join(dir, "b")
	a, err := memnode.NewDurableNode(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableNode(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})

	tid := proto.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	if _, err := a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: participants, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: participants, WriteItems: []proto.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableNode(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.Read(ctx, 0, 1)
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
	a, err := memnode.NewDurableNode(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableNode(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	tid := proto.TransactionID{CoordinatorID: 7, Counter: 4, Epoch: 1}
	participants := []uint32{1, 2}
	if _, err := a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: participants, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 5, Data: []byte("K")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: participants, WriteItems: []proto.WriteItem{{MemID: 2, Addr: 5, Data: []byte("Z")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: false}); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableNode(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.Read(ctx, 5, 1)
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
	n, err := memnode.NewDurableNode(1, 32, filepath.Join(dir, "n"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.ApplyWriteForTest(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	tid := proto.TransactionID{CoordinatorID: 9, Counter: 9, Epoch: 1}
	resp, err := n.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != proto.VoteOK {
		t.Fatalf("expected OK vote, got %v", resp.Vote)
	}
	if n.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected no redo log entries for read-only transaction")
	}
}

func TestRedoLogGCCommitAfterAllApplied(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "a"))
	b, _ := memnode.NewDurableNode(2, 64, filepath.Join(root, "b"))
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})

	tid := proto.TransactionID{CoordinatorID: 1, Counter: 11, Epoch: 1}
	parts := []uint32{1, 2}
	_, _ = a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: parts, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("a")}}})
	_, _ = b.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: parts, WriteItems: []proto.WriteItem{{MemID: 2, Addr: 0, Data: []byte("b")}}})
	_, _ = a.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true})
	_, _ = b.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)

	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 {
		t.Fatalf("expected redo gc to truncate committed entry")
	}
	if a.HasDecidedForTest(tid) {
		t.Fatalf("expected decided entry cleaned")
	}
	if a.HasAppliedTrackingForTest(tid) {
		t.Fatalf("expected applied tracking cleaned")
	}
}

func TestRedoLogGCNoPrematureTruncationWithoutRemoteConfirm(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	tid := proto.TransactionID{CoordinatorID: 1, Counter: 12, Epoch: 1}
	parts := []uint32{1, 2}
	_, _ = a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: parts, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 1, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true})

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
	a, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "a"))
	t.Cleanup(func() { _ = a.Close() })
	tid := proto.TransactionID{CoordinatorID: 1, Counter: 13, Epoch: 1}
	_, _ = a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 2, Data: []byte("a")}}})
	_, _ = a.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: false})

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
	n, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "n"))
	t.Cleanup(func() { _ = n.Close() })
	base := time.Unix(1000, 0)
	n.SetEpochSourceForTest(time.Second, func() time.Time { return base })
	old := proto.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	recent := proto.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 5}
	_, _ = n.RecoveryForceAbort(ctx, proto.RecoverForceAbortRequest{TID: old})
	_, _ = n.RecoveryForceAbort(ctx, proto.RecoverForceAbortRequest{TID: recent})
	base = base.Add(6 * time.Second)

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
	a, _ := memnode.NewDurableNode(1, 64, aDir)
	b, _ := memnode.NewDurableNode(2, 64, bDir)
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	tid := proto.TransactionID{CoordinatorID: 5, Counter: 1, Epoch: 1}
	parts := []uint32{1, 2}
	_, _ = a.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: parts, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 7, Data: []byte("x")}}})
	_, _ = b.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: parts, WriteItems: []proto.WriteItem{{MemID: 2, Addr: 7, Data: []byte("y")}}})
	_, _ = a.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true})
	_, _ = b.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true})
	a.FlushAppliedNotifications(ctx)
	b.FlushAppliedNotifications(ctx)
	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewDurableNode(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.Read(ctx, 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "x" {
		t.Fatalf("expected recovered value x, got %q", got)
	}
}

func TestDurableOnePCWriteApplyFailureRecoversFromRedo(t *testing.T) {
	ctx := context.Background()
	n, err := memnode.NewDurableNode(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	fault := errors.New("injected write apply failure")
	n.SetWriteApplyFaultForTest(func(uint64, []byte) error { return fault })
	tid := proto.TransactionID{CoordinatorID: 1, Counter: 99, Epoch: 1}
	resp, err := n.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{
		TID:        tid,
		WriteItems: []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("redo")}},
	})
	if !errors.Is(err, fault) || resp.Vote != proto.VoteBadInternal {
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
	var read proto.ReadResponse
	err = client.Call(readCtx, proto.MethodRead, proto.ReadRequest{Addr: 4, Len: 4}, &read)
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
	got, err := n.Read(ctx, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "redo" {
		t.Fatalf("recovered value = %q, want redo", got)
	}
	retry, err := n.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{
		RequestID:  "retry-after-recovery",
		TID:        tid,
		WriteItems: []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("redo")}},
	})
	if !errors.Is(err, memnode.ErrReplayUnavailable) || retry.Vote != proto.VoteBadInternal {
		t.Fatalf("exact-TID retry = (%+v, %v), want replay-unavailable failure", retry, err)
	}
	got, err = n.Read(ctx, 4, 4)
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
	got, err = n.Read(ctx, 4, 4)
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
