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
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Recovery decisions from peer evidence, in-flight work, and readiness gating.

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

func TestParticipantRecoveryCommitsFromAllPositiveVotes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if resp, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: participants, HasWrites: true,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare a failed vote=%v err=%v", resp.Vote, err)
	}
	if resp, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: participants, HasWrites: true,
		WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare b failed vote=%v err=%v", resp.Vote, err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("all-positive vote recovery value=%q want=A", got)
	}
}

func TestParticipantRecoveryUsesDurableAbortDecision(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	txId := sinfonia.TransactionID{CoordinatorID: 44, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	for _, prepared := range []struct {
		node   *memnode.Participant
		hostId uint32
		data   string
	}{{a, 1, "A"}, {b, 2, "B"}} {
		resp, err := prepared.node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
			TID: txId, Participants: participants, HasWrites: true,
			WriteItems: []sinfonia.WriteItem{{MemID: prepared.hostId, Addr: 0, Data: []byte(prepared.data)}},
		})
		if err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare memnode %d vote=%v err=%v", prepared.hostId, resp.Vote, err)
		}
	}
	if resp, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil || !resp.Ack {
		t.Fatalf("abort witness resp=%+v err=%v", resp, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a, err = memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a.ReadBytesForTest(0, 1)
	if err != nil || len(got) != 1 || got[0] != 0 {
		t.Fatalf("durable abort decision was ignored: value=%q err=%v", got, err)
	}
	if !a.HasDecidedForTest(txId) || b.HasForcedAbortForTest(txId) {
		t.Fatalf("participant recovery did not use the durable abort decision")
	}
}

func TestLateCommitDoesNotAcknowledgeForcedOrFinalAbort(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		abort func(*testing.T, *memnode.Participant, sinfonia.TransactionID)
	}{
		{
			name: "forced abort",
			abort: func(t *testing.T, node *memnode.Participant, txId sinfonia.TransactionID) {
				t.Helper()
				if err := node.ForceAbortForTest(txId); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "final abort",
			abort: func(t *testing.T, node *memnode.Participant, txId sinfonia.TransactionID) {
				t.Helper()
				if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil || !resp.Ack {
					t.Fatalf("abort resp=%+v err=%v", resp, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := memnode.NewDurableParticipant(1, 64, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })
			txId := sinfonia.TransactionID{CoordinatorID: 80, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID: txId, Participants: []uint32{1, 2}, HasWrites: true,
				WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
			}); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("prepare resp=%+v err=%v", resp, err)
			}
			tc.abort(t, node, txId)

			resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Ack {
				t.Fatalf("late commit acknowledged after %s: %+v", tc.name, resp)
			}
			status, err := node.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: txId})
			if err != nil {
				t.Fatal(err)
			}
			if status.Outcome == sinfonia.TidOutcomeCommit || status.VotedOK {
				t.Fatalf("late commit installed commit evidence after %s: %+v", tc.name, status)
			}
			if got, _ := node.ReadBytesForTest(0, 1); got[0] != 0 {
				t.Fatalf("late commit applied after %s: %q", tc.name, got)
			}
		})
	}
}

func TestForcedAbortRemainsAuthoritativeAcrossClockReversalAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	before := time.Unix(3600, 0)
	now := before
	node.SetClockForTest(func() time.Time { return now })
	txId := sinfonia.TransactionID{CoordinatorID: 81, Counter: 1, Epoch: sinfonia.EpochAt(before)}
	if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: []uint32{1, 2}, HasWrites: true,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	now = before.Add(2 * time.Hour)
	if err := node.ForceAbortForTest(txId); err != nil {
		t.Fatal(err)
	}
	now = before.Add(-time.Hour)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	node.SetClockForTest(func() time.Time { return now })

	vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: txId, Epoch: txId.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if vote.Vote != sinfonia.VoteBadForced {
		t.Fatalf("clock reversal replaced durable forced abort with vote %v", vote.Vote)
	}
	commit, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	if commit.Ack {
		t.Fatalf("clock reversal allowed commit acknowledgement: %+v", commit)
	}
}

type gatedRecoveryReplica struct {
	*memnode.Participant
	entered chan struct{}
	release chan struct{}
}

func (p *gatedRecoveryReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	close(p.entered)
	select {
	case <-ctx.Done():
		return sinfonia.ReplicateRedoEntryResponse{}, ctx.Err()
	case <-p.release:
	}
	return p.Participant.ReplicateRedoEntry(ctx, req)
}

func TestRecoveryDoesNotForceAbortAnInFlightRequest(t *testing.T) {
	for _, onePhase := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare", true: "one-phase"}[onePhase], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			primary, err := memnode.NewDurableParticipant(1, 16, filepath.Join(dir, "primary"))
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			replica, err := memnode.NewDurableParticipant(2, 16, filepath.Join(dir, "replica"))
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replica.ConfigureAsReplica(1)
			gate := &gatedRecoveryReplica{Participant: replica, entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(gate.release) })
			defer release()
			primary.ConfigureAsPrimary(2, gate)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			done := make(chan error, 1)
			go func() {
				writes := []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}
				var vote sinfonia.Vote
				var err error
				if onePhase {
					resp, e := primary.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: writes})
					vote, err = resp.Vote, e
				} else {
					resp, e := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: writes})
					vote, err = resp.Vote, e
				}
				if err == nil && vote != sinfonia.VoteOk {
					err = errors.New("request rejected after recovery race")
				}
				done <- err
			}()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
			_, err = primary.RecoveryForceAbort(short, sinfonia.RecoverForceAbortRequest{TID: tid})
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("recovery should wait for the original request: %v", err)
			}
			release()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			vote, err := primary.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
			if err != nil || vote.Vote != sinfonia.VoteOk {
				t.Fatalf("recovery reversed successful request: %+v, %v", vote, err)
			}
			if resp, err := primary.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true, Generation: vote.Generation}); err != nil || !resp.Ack {
				t.Fatalf("decision: %+v, %v", resp, err)
			}
			got, err := primary.ReadBytesForTest(0, 1)
			if err != nil || string(got) != "A" {
				t.Fatalf("recovered value=%q, %v", got, err)
			}
		})
	}
}

func TestRecoveryAbortRejectsLateOnePhaseRequestOnRAMNode(t *testing.T) {
	node := memnode.NewParticipantWithId(1, 16)
	ctx := context.Background()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
	if err != nil || vote.Vote != sinfonia.VoteBadForced {
		t.Fatalf("force abort: %+v, %v", vote, err)
	}
	response, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}})
	if err != nil || response.Vote != sinfonia.VoteBadForced {
		t.Fatalf("late request reversed abort: %+v, %v", response, err)
	}
	got, _ := node.ReadBytesForTest(0, 1)
	if string(got) != "\x00" {
		t.Fatalf("aborted write applied: %q", got)
	}
}

type unavailableRecoveryPeer struct{}

func (unavailableRecoveryPeer) QueryTidStatus(ctx context.Context, _ sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error) {
	<-ctx.Done()
	return sinfonia.QueryTidStatusResponse{}, ctx.Err()
}

func (unavailableRecoveryPeer) NotifyApplied(context.Context, sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	return sinfonia.NotifyAppliedResponse{}, errors.New("peer is still recovering")
}

type abortRecoveryPeer struct{}

func (abortRecoveryPeer) QueryTidStatus(context.Context, sinfonia.QueryTidStatusRequest) (sinfonia.QueryTidStatusResponse, error) {
	return sinfonia.QueryTidStatusResponse{KnownDecision: true, Outcome: sinfonia.TidOutcomeAbort}, nil
}

func (abortRecoveryPeer) NotifyApplied(context.Context, sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	return sinfonia.NotifyAppliedResponse{Ack: true}, nil
}

type blockingDecisionReplica struct {
	node    *memnode.Participant
	started chan struct{}
	release chan struct{}
}

func (r *blockingDecisionReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	return r.node.ReplicateRedoEntry(ctx, req)
}

func (r *blockingDecisionReplica) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	close(r.started)
	select {
	case <-r.release:
	case <-ctx.Done():
		return sinfonia.ReplicateCommitDecisionResponse{Ack: false}, ctx.Err()
	}
	return r.node.ReplicateCommitDecision(ctx, req)
}

func (r *blockingDecisionReplica) ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
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

	txId := sinfonia.TransactionID{CoordinatorID: 41, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
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
	restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID sinfonia.TransactionID) error {
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
		resp sinfonia.CommitResponse
		err  error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{TID: txId, RequestID: "opposite-race", ActionCommit: false})
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
	got, err := restarted.ReadBytesForTest(8, 1)
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
	resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{TID: txId, RequestID: "same-tid-retry", ActionCommit: true})
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
	got, err = restarted.ReadBytesForTest(8, 1)
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

			txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			participants := []uint32{1, 2}
			if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []sinfonia.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
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
			restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID sinfonia.TransactionID) error {
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
			resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{
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
			got, err := restarted.ReadBytesForTest(8, 1)
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

	txId := sinfonia.TransactionID{CoordinatorID: 43, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if prep, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: []uint32{1, 3},
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("C")}},
	}); err != nil || prep.Vote != sinfonia.VoteOk {
		t.Fatalf("ExecAndPrepare() resp=%+v err=%v", prep, err)
	}
	primary.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{3: abortRecoveryPeer{}})
	replayBlocked := make(chan struct{})
	releaseReplay := make(chan struct{})
	primary.SetRecoverBoundaryHookForTest(func(phase string, hookTID sinfonia.TransactionID) error {
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
		resp sinfonia.CommitResponse
		err  error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		resp, err := primary.Commit(ctx, sinfonia.CommitRequest{TID: txId, RequestID: "replicated-race", ActionCommit: true})
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
	primaryStatus, err := primary.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: txId})
	if err != nil {
		t.Fatal(err)
	}
	if !primaryStatus.KnownDecision || primaryStatus.Outcome != sinfonia.TidOutcomeCommit {
		t.Fatalf("primary recovery changed replicated decision: %+v", primaryStatus)
	}
	close(blocking.release)
	result := <-commitDone
	if result.err != nil || !result.resp.Ack {
		t.Fatalf("Commit() resp=%+v err=%v", result.resp, result.err)
	}
	replicaStatus, err := replica.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: txId})
	if err != nil {
		t.Fatal(err)
	}
	if !replicaStatus.KnownDecision || replicaStatus.Outcome != sinfonia.TidOutcomeCommit {
		t.Fatalf("replica decision disagrees with primary: %+v", replicaStatus)
	}
	got, err := primary.ReadBytesForTest(8, 1)
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

	txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 9, Data: []byte("R")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants}); err != nil {
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
	got, err := restarted.ReadBytesForTest(9, 1)
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

func TestRPCRejectsClientWorkUntilRecoveryCompletes(t *testing.T) {
	node, err := memnode.NewDurableParticipant(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = node.RPCServer().Serve(listener) }()

	client := rpcwire.NewClient(listener.Addr().String(), rpcwire.ClientOptions{DialTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var read sinfonia.ExecCommit1PCResponse
	err = client.Call(ctx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 17, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}, ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}}}, &read)
	if err == nil || !strings.Contains(err.Error(), memnode.ErrRecoveryPending.Error()) {
		t.Fatalf("Read before recovery error = %v, want ErrRecoveryPending", err)
	}

	var entries sinfonia.ListRedoEntriesResponse
	if err := client.Call(ctx, sinfonia.MethodListRedoEntries, sinfonia.ListRedoEntriesRequest{}, &entries); err != nil {
		t.Fatalf("recovery-safe RPC failed while unready: %v", err)
	}

	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(ctx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 17, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}, ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}}}, &read); err != nil {
		t.Fatalf("Read after recovery failed: %v", err)
	}
}
