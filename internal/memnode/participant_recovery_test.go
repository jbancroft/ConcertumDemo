package memnode_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/storage"
)

type unavailableRecoveryPeer struct{}

func (unavailableRecoveryPeer) QueryTidStatus(ctx context.Context, _ proto.QueryTidStatusRequest) (proto.QueryTidStatusResponse, error) {
	<-ctx.Done()
	return proto.QueryTidStatusResponse{}, ctx.Err()
}

func (unavailableRecoveryPeer) NotifyApplied(context.Context, proto.NotifyAppliedRequest) (proto.NotifyAppliedResponse, error) {
	return proto.NotifyAppliedResponse{}, errors.New("peer is still recovering")
}

type abortRecoveryPeer struct{}

func (abortRecoveryPeer) QueryTidStatus(context.Context, proto.QueryTidStatusRequest) (proto.QueryTidStatusResponse, error) {
	return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeAbort}, nil
}

func (abortRecoveryPeer) NotifyApplied(context.Context, proto.NotifyAppliedRequest) (proto.NotifyAppliedResponse, error) {
	return proto.NotifyAppliedResponse{Ack: true}, nil
}

type blockingDecisionReplica struct {
	node    *memnode.Participant
	started chan struct{}
	release chan struct{}
}

func (r *blockingDecisionReplica) ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	return r.node.ReplicateRedoEntry(ctx, req)
}

func (r *blockingDecisionReplica) ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	close(r.started)
	select {
	case <-r.release:
	case <-ctx.Done():
		return proto.ReplicateCommitDecisionResponse{Ack: false}, ctx.Err()
	}
	return r.node.ReplicateCommitDecision(ctx, req)
}

func (r *blockingDecisionReplica) ReplicateReplayRecord(ctx context.Context, req proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	return r.node.ReplicateReplayRecord(ctx, req)
}

func TestParticipantRecoveryWaitsForMissingPeerEvidence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	bDir := filepath.Join(root, "b")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := memnode.NewDurableParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	txId := proto.TransactionID{CoordinatorID: 41, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []proto.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	server := httptest.NewServer(restarted.HTTPHandler(false))
	t.Cleanup(server.Close)
	assertReadyStatus(t, server.URL, http.StatusServiceUnavailable)

	err = restarted.Recover(ctx)
	if !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v, want ErrRecoveryPending", err)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("missing peer evidence must not persist an abort decision")
	}
	processed, err := storage.LoadProcessedPointer(aDir)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("processed pointer = %d, want 0 while recovery is pending", processed)
	}
	assertReadyStatus(t, server.URL, http.StatusServiceUnavailable)

	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	applyBlocked := make(chan struct{})
	releaseApply := make(chan struct{})
	restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID proto.TransactionID) error {
		if phase == "apply-before" && hookTID == txId {
			close(applyBlocked)
			<-releaseApply
		}
		return nil
	})
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- restarted.Recover(ctx) }()
	select {
	case <-applyBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not reach apply boundary")
	}
	decisionPath := filepath.Join(aDir, "decided.log")
	decisionBefore, err := os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	type commitResult struct {
		resp proto.CommitResponse
		err  error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		resp, err := restarted.Commit(ctx, proto.CommitRequest{TID: txId, RequestID: "opposite-race", ActionCommit: false})
		commitDone <- commitResult{resp: resp, err: err}
	}()
	select {
	case result := <-commitDone:
		t.Fatalf("concurrent Commit returned before recovery terminalization: resp=%+v err=%v", result.resp, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseApply)
	if err := <-recoverDone; err != nil {
		t.Fatal(err)
	}
	result := <-commitDone
	if result.err != nil || !result.resp.Ack {
		t.Fatalf("opposite phase-two race resp=%+v err=%v", result.resp, result.err)
	}
	decisionAfter, err := os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decisionBefore, decisionAfter) {
		t.Fatalf("opposite phase-two race changed recovery decision: before=%q after=%q", decisionBefore, decisionAfter)
	}
	got, err := restarted.Read(ctx, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("recovered value = %q, want A", got)
	}
	decisionBefore, err = os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(8, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	resp, err := restarted.Commit(ctx, proto.CommitRequest{TID: txId, RequestID: "same-tid-retry", ActionCommit: true})
	if err != nil || !resp.Ack {
		t.Fatalf("same-TID commit retry resp=%+v err=%v", resp, err)
	}
	decisionAfter, err = os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decisionBefore, decisionAfter) {
		t.Fatalf("same-TID retry mutated decided state: before=%q after=%q", decisionBefore, decisionAfter)
	}
	got, err = restarted.Read(ctx, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("same-TID retry reapplied old write: got %q want Z", got)
	}
	assertReadyStatus(t, server.URL, http.StatusOK)
}

func TestParticipantRecoveryFailurePreservesDurableDecision(t *testing.T) {
	for _, failurePhase := range []string{"apply-before", "apply-after", "pointer-before"} {
		t.Run(failurePhase, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			aDir := filepath.Join(root, "a")
			bDir := filepath.Join(root, "b")
			a, err := memnode.NewDurableParticipant(1, 64, aDir)
			if err != nil {
				t.Fatal(err)
			}
			b, err := memnode.NewDurableParticipant(2, 64, bDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })

			txId := proto.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: 1}
			participants := []uint32{1, 2}
			if _, err := a.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []proto.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := memnode.NewDurableParticipant(1, 64, aDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
			injected := errors.New("injected recovery terminalization failure")
			restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID proto.TransactionID) error {
				if phase == failurePhase && hookTID == txId {
					return injected
				}
				return nil
			})
			if err := restarted.Recover(ctx); !errors.Is(err, injected) {
				t.Fatalf("Recover() error = %v, want %v", err, injected)
			}

			decisionPath := filepath.Join(aDir, "decided.log")
			decisionBefore, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := restarted.Commit(ctx, proto.CommitRequest{
				TID:          txId,
				RequestID:    "opposite-after-recovery-failure",
				ActionCommit: false,
			})
			if err != nil || !resp.Ack {
				t.Fatalf("opposite Commit resp=%+v err=%v", resp, err)
			}
			decisionAfter, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decisionBefore, decisionAfter) {
				t.Fatalf("opposite Commit replaced durable recovery decision: before=%q after=%q", decisionBefore, decisionAfter)
			}
			got, err := restarted.Read(ctx, 8, 1)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "A" {
				t.Fatalf("normalized Commit value = %q, want A", got)
			}

			restarted.SetRecoverBoundaryHookForTest(nil)
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			decisionAfterRetry, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decisionBefore, decisionAfterRetry) {
				t.Fatalf("recovery retry replaced durable decision: before=%q after=%q", decisionBefore, decisionAfterRetry)
			}
		})
	}
}

func TestReplicatedCommitDecisionIsDurableBeforeRecoveryRace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	blocking := &blockingDecisionReplica{
		node:    replica,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	primary.ConfigureAsPrimary(2, blocking)

	txId := proto.TransactionID{CoordinatorID: 43, Counter: 1, Epoch: 1}
	if prep, err := primary.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID:          txId,
		Participants: []uint32{1, 3},
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("C")}},
	}); err != nil || prep.Vote != proto.VoteOk {
		t.Fatalf("ExecAndPrepare() resp=%+v err=%v", prep, err)
	}
	primary.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{3: abortRecoveryPeer{}})
	replayBlocked := make(chan struct{})
	releaseReplay := make(chan struct{})
	primary.SetRecoverBoundaryHookForTest(func(phase string, hookTID proto.TransactionID) error {
		if phase == "replay-before" && hookTID == txId {
			close(replayBlocked)
			<-releaseReplay
		}
		return nil
	})
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- primary.Recover(ctx) }()
	select {
	case <-replayBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not reach replay boundary")
	}

	type commitResult struct {
		resp proto.CommitResponse
		err  error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		resp, err := primary.Commit(ctx, proto.CommitRequest{TID: txId, RequestID: "replicated-race", ActionCommit: true})
		commitDone <- commitResult{resp: resp, err: err}
	}()
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Commit did not reach blocking replica")
	}
	close(releaseReplay)
	if err := <-recoverDone; err != nil {
		t.Fatal(err)
	}
	primaryStatus, err := primary.QueryTidStatus(ctx, proto.QueryTidStatusRequest{TID: txId})
	if err != nil {
		t.Fatal(err)
	}
	if !primaryStatus.KnownDecision || primaryStatus.Outcome != proto.TidOutcomeCommit {
		t.Fatalf("primary recovery changed replicated decision: %+v", primaryStatus)
	}
	close(blocking.release)
	result := <-commitDone
	if result.err != nil || !result.resp.Ack {
		t.Fatalf("Commit() resp=%+v err=%v", result.resp, result.err)
	}
	replicaStatus, err := replica.QueryTidStatus(ctx, proto.QueryTidStatusRequest{TID: txId})
	if err != nil {
		t.Fatal(err)
	}
	if !replicaStatus.KnownDecision || replicaStatus.Outcome != proto.TidOutcomeCommit {
		t.Fatalf("replica decision disagrees with primary: %+v", replicaStatus)
	}
	got, err := primary.Read(ctx, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "C" {
		t.Fatalf("primary recovered value = %q, want C", got)
	}
}

func TestParticipantRecoveryRetriesUnavailablePeer(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	b := memnode.NewParticipantWithId(2, 64)

	txId := proto.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 9, Data: []byte("R")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, proto.ExecPrepareRequest{TID: txId, Participants: participants}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.SetPeerDialer(nil, 10*time.Millisecond)
	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: unavailableRecoveryPeer{}})
	if err := restarted.Recover(ctx); !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v, want ErrRecoveryPending", err)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("unreachable peer must not persist an abort decision")
	}

	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Read(ctx, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "R" {
		t.Fatalf("recovered value = %q, want R", got)
	}
}

func assertReadyStatus(t *testing.T, baseURL string, want int) {
	t.Helper()
	resp, err := http.Get(baseURL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("ready status = %d, want %d", resp.StatusCode, want)
	}
}
