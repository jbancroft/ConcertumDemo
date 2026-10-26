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

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
)

// phaseTwoNode controls delivery at the existing MemNode interface while the
// coordinator runs its normal asynchronous workers and batching implementation.
type phaseTwoNode struct {
	MemNode
	prepared  chan proto.TransactionID
	delivered chan proto.CommitRequest
	gate      <-chan struct{}
	batchErr  error
}

func newPhaseTwoNode(node MemNode, gate <-chan struct{}) *phaseTwoNode {
	return &phaseTwoNode{
		MemNode: node, prepared: make(chan proto.TransactionID, 2),
		delivered: make(chan proto.CommitRequest, 2), gate: gate,
	}
}

func (n *phaseTwoNode) ExecPrepare(ctx context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error) {
	resp, err := n.MemNode.ExecPrepare(ctx, req)
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

func (n *phaseTwoNode) Commit(ctx context.Context, req proto.CommitRequest) (proto.CommitResponse, error) {
	if err := n.waitForDelivery(ctx); err != nil {
		return proto.CommitResponse{}, err
	}
	resp, err := n.MemNode.Commit(ctx, req)
	if err == nil && resp.Ack {
		n.delivered <- req
	}
	return resp, err
}

func (n *phaseTwoNode) CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	if n.batchErr != nil {
		return proto.CommitBatchResponse{}, n.batchErr
	}
	if err := n.waitForDelivery(ctx); err != nil {
		return proto.CommitBatchResponse{}, err
	}
	resp, err := n.MemNode.CommitBatch(ctx, req)
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

func phaseTwoWrites() []proto.WriteItem {
	return []proto.WriteItem{
		{MemID: 1, Addr: 8, Data: []byte("A")},
		{MemID: 2, Addr: 8, Data: []byte("B")},
	}
}

func TestCoordinatorPhaseTwoUsesGeneratedIDsAndAsynchronousDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	n1 := newPhaseTwoNode(memnode.NewNode(64), nil)
	n2 := newPhaseTwoNode(memnode.NewNode(64), gate)
	coord := NewCoordinator(map[uint32]MemNode{1: n1, 2: n2})

	result := coord.ExecAndCommit(ctx, "", nil, nil, phaseTwoWrites())
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
	if got, err := n2.Read(ctx, 8, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("blocked participant changed before delivery: %q, err=%v", got, err)
	}
	release()
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	if got := receivePhaseTwo(t, ctx, n2.delivered); got.TID != first || !got.ActionCommit {
		t.Fatalf("second delivery=%+v, want commit for %+v", got, first)
	}
	if got, err := n2.Read(ctx, 8, 1); err != nil || string(got) != "B" {
		t.Fatalf("delivered write=%q, err=%v", got, err)
	}

	result = coord.ExecAndCommit(ctx, "", nil, nil, phaseTwoWrites())
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
	n1 := newPhaseTwoNode(memnode.NewNode(64), nil)
	n2 := newPhaseTwoNode(memnode.NewNode(64), nil)
	coord := NewCoordinator(map[uint32]MemNode{1: n1, 2: n2})
	result := coord.ExecAndCommit(ctx, "",
		[]proto.CmpItem{{MemID: 2, Addr: 8, Data: []byte("stale")}}, nil, phaseTwoWrites())
	if result.Committed || result.UnknownOutcome || result.Status != proto.VoteBadCmp {
		t.Fatalf("expected compare abort, got %+v", result)
	}
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*phaseTwoNode{n1, n2} {
		tid := receivePhaseTwo(t, ctx, node.prepared)
		if got := receivePhaseTwo(t, ctx, node.delivered); got.TID != tid || got.ActionCommit {
			t.Fatalf("delivery=%+v, want abort for %+v", got, tid)
		}
		if got, err := node.Read(ctx, 8, 1); err != nil || string(got) != "\x00" {
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
			var tid proto.TransactionID
			if err := json.Unmarshal(data, &tid); err != nil {
				t.Fatal(err)
			}
			if tid.CoordinatorID == 0 || tid.Counter == 0 || tid.Epoch == 0 {
				t.Fatalf("invalid captured transaction ID: %+v", tid)
			}

			nodes := openPhaseTwoNodes(t, dir)
			for i, node := range nodes {
				if err := node.Recover(ctx); err != nil {
					t.Fatalf("recover participant %d: %v", i+1, err)
				}
				want := phaseTwoWrites()[i].Data
				if got, err := node.Read(ctx, 8, 1); err != nil || string(got) != string(want) {
					t.Fatalf("participant %d recovered %q, want %q, err=%v", i+1, got, want, err)
				}
			}
		})
	}
}

func openPhaseTwoNodes(t *testing.T, dir string) []*memnode.Node {
	t.Helper()
	var nodes []*memnode.Node
	for id := uint32(1); id <= 2; id++ {
		node, err := memnode.NewDurableNode(id, 64, filepath.Join(dir, fmt.Sprintf("node-%d", id)))
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
	coord := NewCoordinator(map[uint32]MemNode{1: n1, 2: n2})
	if result := coord.ExecAndCommit(ctx, "", nil, nil, phaseTwoWrites()); !result.Committed || result.UnknownOutcome {
		t.Fatalf("result=%+v", result)
	}
	delivered := receivePhaseTwo(t, ctx, n1.delivered)
	for _, node := range []*phaseTwoNode{n1, n2} {
		if tid := receivePhaseTwo(t, ctx, node.prepared); tid != delivered.TID {
			t.Fatalf("prepare=%+v, delivered=%+v", tid, delivered)
		}
	}
	for i, want := range []string{"A", "\x00"} {
		if got, err := nodes[i].Read(ctx, 8, 1); err != nil || string(got) != want {
			t.Fatalf("participant %d before crash: %q, want %q, err=%v", i+1, got, want, err)
		}
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
