package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

// Asynchronous commit and abort delivery, batching fallback, flushing, and interrupted delivery.

func TestCoordinatorCloseDrainsDecisions(t *testing.T) {
	for _, mode := range []string{"commit", "abort", "delivery failure"} {
		t.Run(mode, func(t *testing.T) {
			n1 := memnode.NewParticipant(16)
			n2 := memnode.NewParticipant(16)
			nodes := map[uint32]Participant{1: n1, 2: n2}
			if mode == "delivery failure" {
				nodes[2] = &failingCommitNode{Participant: n2}
			}
			coord := NewCoordinator(nodes)
			defer coord.Close()
			var compares []sinfonia.CmpItem
			want := "A"
			if mode == "abort" {
				compares = []sinfonia.CmpItem{{MemID: 2, Data: []byte("stale")}}
				want = "\x00"
			}
			result := coord.ExecuteAndCommit(context.Background(), compares, nil, twoParticipantWrites())
			if result.Committed != (mode != "abort") || result.UnknownOutcome {
				t.Fatalf("unexpected decision: %+v", result)
			}
			for range 2 {
				if err := coord.Close(); (err != nil) != (mode == "delivery failure") {
					t.Fatalf("close error=%v for %s", err, mode)
				}
			}
			// Delivery must finish at the reachable node and release its locks.
			got, err := readParticipant(context.Background(), n1, 8, 1)
			if err != nil || string(got) != want {
				t.Fatalf("value after Close=%q, want %q; error=%v", got, want, err)
			}
		})
	}
}

func TestFlushPendingCommitsReportsEarlierTimerDeliveryFailure(t *testing.T) {
	failing := &failingCommitNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{
		1: failing,
		2: memnode.NewParticipant(16),
	})
	defer coord.Close()
	res := coord.ExecuteAndCommit(context.Background(), nil, nil, []sinfonia.WriteItem{
		{MemID: 1, Addr: 0, Data: []byte("A")},
		{MemID: 2, Addr: 0, Data: []byte("B")},
	})
	if !res.Committed {
		t.Fatalf("expected commit decision before phase-two delivery: %+v", res)
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coord.FlushPendingCommits(ctx); err == nil {
		t.Fatal("expected flush to report the earlier phase-two delivery failure")
	}
}

func TestFlushPendingCommitsReportsUnacknowledgedFallbackDelivery(t *testing.T) {
	unacknowledged := &unacknowledgedFallbackCommitNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{
		1: unacknowledged,
		2: memnode.NewParticipant(16),
	})
	defer coord.Close()
	res := coord.ExecuteAndCommit(context.Background(), nil, nil, []sinfonia.WriteItem{
		{MemID: 1, Addr: 0, Data: []byte("A")},
		{MemID: 2, Addr: 0, Data: []byte("B")},
	})
	if !res.Committed {
		t.Fatalf("expected commit decision before phase-two delivery: %+v", res)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coord.FlushPendingCommits(ctx); err == nil {
		t.Fatal("expected flush to report the unacknowledged fallback delivery")
	}
}

func TestCoordinatorPhaseTwoUsesGeneratedIDsAndAsynchronousDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	n1 := newPhaseTwoNode(memnode.NewParticipant(64), nil)
	n2 := newPhaseTwoNode(memnode.NewParticipant(64), gate)
	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer func() { release(); _ = coord.Close() }()

	result := coord.ExecuteAndCommit(ctx, nil, nil, twoParticipantWrites())
	if !result.Committed || result.UnknownOutcome {
		t.Fatalf("result=%+v", result)
	}
	first := receivePhaseTwo(t, ctx, n1.prepared)
	if got := receivePhaseTwo(t, ctx, n2.prepared); got != first {
		t.Fatalf("participants prepared different transactions: %v and %v", first, got)
	}
	if first.CoordinatorID == 0 || first.Counter == 0 || first.Epoch == 0 {
		t.Fatalf("invalid generated transaction ID: %+v", first)
	}
	if got := receivePhaseTwo(t, ctx, n1.delivered); got.TID != first || !got.ActionCommit {
		t.Fatalf("first delivery=%+v, want commit for %+v", got, first)
	}
	if got, err := readParticipant(ctx, n2, 8, 1); !errors.Is(err, ErrTransactionAborted) || got != nil {
		t.Fatalf("read bypassed prepared write before delivery: %q, err=%v", got, err)
	}
	release()
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	if got := receivePhaseTwo(t, ctx, n2.delivered); got.TID != first || !got.ActionCommit {
		t.Fatalf("second delivery=%+v, want commit for %+v", got, first)
	}
	if got, err := readParticipant(ctx, n2, 8, 1); err != nil || string(got) != "B" {
		t.Fatalf("delivered write=%q, err=%v", got, err)
	}

	result = coord.ExecuteAndCommit(ctx, nil, nil, twoParticipantWrites())
	if !result.Committed || result.UnknownOutcome {
		t.Fatalf("second result=%+v", result)
	}
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	second := receivePhaseTwo(t, ctx, n1.prepared)
	if second.CoordinatorID != first.CoordinatorID || second.Counter <= first.Counter {
		t.Fatalf("transaction ID did not advance: first=%+v second=%+v", first, second)
	}
	if got := receivePhaseTwo(t, ctx, n2.prepared); got != second {
		t.Fatalf("second prepare=%+v, want %+v", got, second)
	}
	for _, node := range []*phaseTwoNode{n1, n2} {
		if got := receivePhaseTwo(t, ctx, node.delivered); got.TID != second || !got.ActionCommit {
			t.Fatalf("delivery=%+v, want commit for %+v", got, second)
		}
	}
}

func TestCoordinatorPhaseTwoDeliversAbortAfterRejectedPrepare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n1 := newPhaseTwoNode(memnode.NewParticipant(64), nil)
	n2 := newPhaseTwoNode(memnode.NewParticipant(64), nil)
	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer coord.Close()
	result := coord.ExecuteAndCommit(ctx, []sinfonia.CmpItem{{MemID: 2, Addr: 8, Data: []byte("stale")}}, nil, twoParticipantWrites())
	if result.Committed || result.UnknownOutcome || result.Status != sinfonia.VoteBadCmp {
		t.Fatalf("expected compare abort, got %+v", result)
	}
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*phaseTwoNode{n1, n2} {
		txId := receivePhaseTwo(t, ctx, node.prepared)
		if got := receivePhaseTwo(t, ctx, node.delivered); got.TID != txId || got.ActionCommit {
			t.Fatalf("delivery=%+v, want abort for %+v", got, txId)
		}
		if got, err := readParticipant(ctx, node, 8, 1); err != nil || string(got) != "\x00" {
			t.Fatalf("aborted write was applied: %q, err=%v", got, err)
		}
	}
}

func TestCoordinatorRecoversAfterPartialCommitDelivery(t *testing.T) {
	if dir := os.Getenv("CONCERTUM_PHASE_TWO_CRASH_DIR"); dir != "" {
		runPhaseTwoCrashHelper(t, dir)
		return
	}
	for _, mode := range []string{"batch", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCoordinatorRecoversAfterPartialCommitDelivery$")
			cmd.Env = append(os.Environ(), "CONCERTUM_PHASE_TWO_CRASH_DIR="+dir, "CONCERTUM_PHASE_TWO_DELIVERY="+mode)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
				t.Fatalf("helper did not exit at the partial-delivery checkpoint: %v\n%s", err, output)
			}
			data, err := os.ReadFile(filepath.Join(dir, "transaction.json"))
			if err != nil {
				t.Fatal(err)
			}
			var txId sinfonia.TransactionID
			if err := json.Unmarshal(data, &txId); err != nil {
				t.Fatal(err)
			}
			if txId.CoordinatorID == 0 || txId.Counter == 0 || txId.Epoch == 0 {
				t.Fatalf("invalid captured transaction ID: %+v", txId)
			}

			nodes := openPhaseTwoNodes(t, dir)
			for i, node := range nodes {
				if err := node.Recover(ctx); err != nil {
					t.Fatalf("recover participant %d: %v", i+1, err)
				}
				want := twoParticipantWrites()[i].Data
				if got, err := readParticipant(ctx, node, 8, 1); err != nil || string(got) != string(want) {
					t.Fatalf("participant %d recovered %q, want %q, err=%v", i+1, got, want, err)
				}
			}
		})
	}
}

type failingCommitNode struct {
	*memnode.Participant
}

type unacknowledgedFallbackCommitNode struct {
	*memnode.Participant
}

func (n *failingCommitNode) Commit(context.Context, sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return sinfonia.CommitResponse{}, errors.New("commit unavailable")
}

func (n *failingCommitNode) CommitBatch(context.Context, sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	return sinfonia.CommitBatchResponse{}, errors.New("commit batch unavailable")
}

func (n *unacknowledgedFallbackCommitNode) Commit(_ context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	return sinfonia.CommitResponse{RequestID: req.RequestID, Ack: false}, nil
}

func (n *unacknowledgedFallbackCommitNode) CommitBatch(context.Context, sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	return sinfonia.CommitBatchResponse{}, errors.New("commit batch unavailable")
}

// phaseTwoNode intercepts node calls so tests can delay or fail delivery while
// the coordinator uses its normal workers and batching.
type phaseTwoNode struct {
	Participant
	prepared  chan sinfonia.TransactionID
	delivered chan sinfonia.CommitRequest
	gate      <-chan struct{}
	batchErr  error
}

func newPhaseTwoNode(node Participant, gate <-chan struct{}) *phaseTwoNode {
	return &phaseTwoNode{
		Participant: node, prepared: make(chan sinfonia.TransactionID, 2),
		delivered: make(chan sinfonia.CommitRequest, 2), gate: gate,
	}
}

func (n *phaseTwoNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	resp, err := n.Participant.ExecAndPrepare(ctx, req)
	n.prepared <- req.TID
	return resp, err
}

func (n *phaseTwoNode) waitForDelivery(ctx context.Context) error {
	if n.gate == nil {
		return nil
	}
	select {
	case <-n.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *phaseTwoNode) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	if err := n.waitForDelivery(ctx); err != nil {
		return sinfonia.CommitResponse{}, err
	}
	resp, err := n.Participant.Commit(ctx, req)
	if err == nil && resp.Ack {
		n.delivered <- req
	}
	return resp, err
}

func (n *phaseTwoNode) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	if n.batchErr != nil {
		return sinfonia.CommitBatchResponse{}, n.batchErr
	}
	if err := n.waitForDelivery(ctx); err != nil {
		return sinfonia.CommitBatchResponse{}, err
	}
	resp, err := n.Participant.CommitBatch(ctx, req)
	if err == nil {
		for i, decision := range req.Decisions {
			if i < len(resp.Acks) && resp.Acks[i] {
				n.delivered <- decision
			}
		}
	}
	return resp, err
}

func receivePhaseTwo[T any](t *testing.T, ctx context.Context, events <-chan T) T {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func openPhaseTwoNodes(t *testing.T, dir string) []*memnode.Participant {
	t.Helper()
	var nodes []*memnode.Participant
	for id := uint32(1); id <= 2; id++ {
		node, err := memnode.NewDurableParticipant(id, 64, filepath.Join(dir, fmt.Sprintf("node-%d", id)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := node.Close(); err != nil {
				t.Error(err)
			}
		})
		nodes = append(nodes, node)
	}
	nodes[0].SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: nodes[1]})
	nodes[1].SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: nodes[0]})
	return nodes
}

func runPhaseTwoCrashHelper(t *testing.T, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nodes := openPhaseTwoNodes(t, dir)
	for _, node := range nodes {
		if err := node.Recover(ctx); err != nil {
			t.Fatal(err)
		}
	}
	gate := make(chan struct{})
	n1 := newPhaseTwoNode(nodes[0], nil)
	n2 := newPhaseTwoNode(nodes[1], gate)
	if os.Getenv("CONCERTUM_PHASE_TWO_DELIVERY") == "fallback" {
		n1.batchErr = errors.New("force individual commit fallback")
		n2.batchErr = n1.batchErr
	}
	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer func() { close(gate); _ = coord.Close() }()
	if result := coord.ExecuteAndCommit(ctx, nil, nil, twoParticipantWrites()); !result.Committed || result.UnknownOutcome {
		t.Fatalf("result=%+v", result)
	}
	delivered := receivePhaseTwo(t, ctx, n1.delivered)
	for _, node := range []*phaseTwoNode{n1, n2} {
		if txId := receivePhaseTwo(t, ctx, node.prepared); txId != delivered.TID {
			t.Fatalf("prepare=%+v, delivered=%+v", txId, delivered)
		}
	}
	if got, err := readParticipant(ctx, nodes[0], 8, 1); err != nil || string(got) != "A" {
		t.Fatalf("committed participant before crash: %q, err=%v", got, err)
	}
	if got, err := readParticipant(ctx, nodes[1], 8, 1); !errors.Is(err, ErrTransactionAborted) || got != nil {
		t.Fatalf("read bypassed prepared participant before crash: %q, err=%v", got, err)
	}
	data, err := json.Marshal(delivered.TID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "transaction.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	// Exit without flushing the coordinator or closing either durable node.
	// Participant 2's delivery is still gated, so recovery must finish its write.
	os.Exit(23)
}
