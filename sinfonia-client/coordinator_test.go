package sinfoniaclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type countingNode struct {
	client.Participant // Nil uses protocol replies without storage I/O.
	mu                 sync.Mutex
	execPrepareN       int
	execCommit1PCN     int
	commitN            int
	commitBatchN       int
	prepared           map[sinfonia.TransactionID]int
	decided            map[sinfonia.TransactionID]int
	largestBatch       int
	commitGate         <-chan struct{}
}

func (n *countingNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.mu.Lock()
	n.execPrepareN++
	if n.prepared == nil {
		n.prepared = make(map[sinfonia.TransactionID]int)
	}
	n.prepared[req.TID]++
	n.mu.Unlock()
	if n.Participant == nil {
		return sinfonia.ExecPrepareResponse{Vote: sinfonia.VoteOk}, nil
	}
	return n.Participant.ExecAndPrepare(ctx, req)
}

func (n *countingNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.mu.Lock()
	n.execCommit1PCN++
	n.mu.Unlock()
	if n.Participant == nil {
		return sinfonia.ExecCommit1PCResponse{Vote: sinfonia.VoteOk}, nil
	}
	return n.Participant.ExecCommit1PC(ctx, req)
}

func (n *countingNode) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	n.mu.Lock()
	n.commitN++
	n.mu.Unlock()
	if n.Participant == nil {
		return sinfonia.CommitResponse{Ack: true}, nil
	}
	return n.Participant.Commit(ctx, req)
}

func (n *countingNode) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	n.mu.Lock()
	n.commitBatchN++
	n.largestBatch = max(n.largestBatch, len(req.Decisions))
	if n.decided == nil {
		n.decided = make(map[sinfonia.TransactionID]int)
	}
	for _, decision := range req.Decisions {
		if decision.ActionCommit {
			n.decided[decision.TID]++
		}
	}
	n.mu.Unlock()
	if n.commitGate != nil {
		<-n.commitGate
	}
	if n.Participant == nil {
		acks := make([]bool, len(req.Decisions))
		for i := range acks {
			acks[i] = true
		}
		return sinfonia.CommitBatchResponse{Acks: acks}, nil
	}
	return n.Participant.CommitBatch(ctx, req)
}

func assertDecisionsDelivered(t *testing.T, n *countingNode, count int) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.prepared) != count || len(n.decided) != count {
		t.Fatalf("prepared=%d decided=%d, want %d each", len(n.prepared), len(n.decided), count)
	}
	for tid, prepares := range n.prepared {
		if prepares != 1 || n.decided[tid] != 1 {
			t.Fatalf("transaction %v: prepares=%d commits=%d", tid, prepares, n.decided[tid])
		}
	}
}

func TestSingleParticipantUses1PCFastPath(t *testing.T) {
	n := &countingNode{Participant: newTestParticipant(t, 1, 64)}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n})
	defer coord.Close()
	c := &Client{coordinator: coord}

	execution := c.NewMiniTransaction().Write(1, 0, []byte("x")).ExecuteAndCommit(t.Context())

	committed, status := execution.Committed, execution.Status
	if !committed || status != StatusCommitted {
		t.Fatalf("expected commit, got committed=%v status=%v", committed, status)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.execCommit1PCN == 0 {
		t.Fatalf("expected ExecCommit1PC RPC usage")
	}
	if n.execPrepareN != 0 || n.commitN != 0 || n.commitBatchN != 0 {
		t.Fatalf("expected no 2PC path, got prepare=%d commit=%d commitBatch=%d", n.execPrepareN, n.commitN, n.commitBatchN)
	}
}

func TestCoordinatorPipeliningConcurrentTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	n1, n2 := &countingNode{}, &countingNode{}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	const txns = 64
	var wg sync.WaitGroup
	for i := range txns {
		wg.Go(func() {
			execution := c.NewMiniTransaction().
				Write(1, uint64(i), []byte{byte(i)}).
				Write(2, uint64(i), []byte{byte(i)}).
				ExecuteAndCommit(ctx)
			committed := execution.Committed
			if !committed {
				t.Errorf("txn %d aborted", i)
			}
		})
	}
	// Finish all API calls before Close, including when the context expires.
	wg.Wait()
	if err := c.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	assertDecisionsDelivered(t, n1, txns)
	assertDecisionsDelivered(t, n2, txns)
}

func TestCommitMessageBatchingReducesIndividualCommitRPCs(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	n1 := &countingNode{commitGate: gate}
	n2 := &countingNode{commitGate: gate}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	defer release()
	c := &Client{coordinator: coord}

	const txns = 200
	for i := range txns {
		execution := c.NewMiniTransaction().Write(1, uint64(i%64), []byte("a")).Write(2, uint64(i%64), []byte("b")).ExecuteAndCommit(t.Context())
		committed := execution.Committed
		if !committed {
			t.Fatalf("unexpected abort at %d", i)
		}
	}
	// Hold the first delivery until all decisions are queued. Subsequent batches
	// must combine transactions, regardless of scheduling or machine speed.
	release()
	if err := c.FlushPendingCommits(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertDecisionsDelivered(t, n1, txns)
	assertDecisionsDelivered(t, n2, txns)

	n1.mu.Lock()
	b1, c1, max1 := n1.commitBatchN, n1.commitN, n1.largestBatch
	n1.mu.Unlock()
	n2.mu.Lock()
	b2, c2, max2 := n2.commitBatchN, n2.commitN, n2.largestBatch
	n2.mu.Unlock()

	if b1 == 0 || b2 == 0 || b1 >= txns || b2 >= txns || max1 < 2 || max2 < 2 {
		t.Fatalf("expected combined commit RPCs, got batches=%d/%d largest=%d/%d", b1, b2, max1, max2)
	}
	if c1 != 0 || c2 != 0 {
		t.Fatalf("expected no fallback commit RPC, got c1=%d c2=%d", c1, c2)
	}
}

func TestFlushPendingCommitsAppliesQueuedDecisions(t *testing.T) {
	n1 := newTestParticipant(t, 1, 64)
	n2 := newTestParticipant(t, 2, 64)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	execution := c.NewMiniTransaction().
		Write(1, 4, []byte("one")).
		Write(2, 8, []byte("two")).
		ExecuteAndCommit(t.Context())

	committed, status := execution.Committed, execution.Status
	if !committed || status != StatusCommitted {
		t.Fatalf("unexpected transaction outcome: committed=%v status=%v", committed, status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := c.FlushPendingCommits(ctx); err != nil {
		t.Fatalf("flush pending commits: %v", err)
	}
	if got, _ := c.Read(t.Context(), 1, 4, 3); string(got) != "one" {
		t.Fatalf("node 1 write not applied after flush: %q", got)
	}
	if got, _ := c.Read(t.Context(), 2, 8, 3); string(got) != "two" {
		t.Fatalf("node 2 write not applied after flush: %q", got)
	}
}
