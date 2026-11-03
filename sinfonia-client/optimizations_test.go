package sinfoniaclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type countingNode struct {
	*memnode.Participant
	mu             sync.Mutex
	execPrepareN   int
	execCommit1PCN int
	commitN        int
	commitBatchN   int
}

func (n *countingNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.mu.Lock()
	n.execPrepareN++
	n.mu.Unlock()
	return n.Participant.ExecAndPrepare(ctx, req)
}

func (n *countingNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.mu.Lock()
	n.execCommit1PCN++
	n.mu.Unlock()
	return n.Participant.ExecCommit1PC(ctx, req)
}

func (n *countingNode) Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error) {
	n.mu.Lock()
	n.commitN++
	n.mu.Unlock()
	return n.Participant.Commit(ctx, req)
}

func (n *countingNode) CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error) {
	n.mu.Lock()
	n.commitBatchN++
	n.mu.Unlock()
	return n.Participant.CommitBatch(ctx, req)
}

func TestSingleParticipantUses1PCFastPath(t *testing.T) {
	n := &countingNode{Participant: newTestParticipant(t, 1, 64)}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n})
	defer coord.Close()
	c := &Client{coordinator: coord}

	execution := c.NewMiniTransaction().Write(1, 0, []byte("x")).ExecuteAndCommit(context.Background())

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n1 := newTestParticipant(t, 1, 4096)
	n2 := newTestParticipant(t, 2, 4096)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	const txns = 64
	var wg sync.WaitGroup
	wg.Add(txns)
	for i := 0; i < txns; i++ {
		i := i
		go func() {
			defer wg.Done()
			execution := c.NewMiniTransaction().
				Write(1, uint64(i), []byte{byte(i)}).
				Write(2, uint64(i), []byte{byte(i)}).
				ExecuteAndCommit(ctx)
			committed := execution.Committed
			if !committed {
				t.Errorf("txn %d aborted", i)
			}
		}()
	}
	// Finish all API calls before Close, including when the context expires.
	wg.Wait()
}

func TestCommitMessageBatchingReducesIndividualCommitRPCs(t *testing.T) {
	n1 := &countingNode{Participant: newTestParticipant(t, 1, 256)}
	n2 := &countingNode{Participant: newTestParticipant(t, 2, 256)}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	for i := 0; i < 200; i++ {
		execution := c.NewMiniTransaction().Write(1, uint64(i%64), []byte("a")).Write(2, uint64(i%64), []byte("b")).ExecuteAndCommit(context.Background())
		committed := execution.Committed
		if !committed {
			t.Fatalf("unexpected abort at %d", i)
		}
	}
	time.Sleep(50 * time.Millisecond)

	n1.mu.Lock()
	b1, c1 := n1.commitBatchN, n1.commitN
	n1.mu.Unlock()
	n2.mu.Lock()
	b2, c2 := n2.commitBatchN, n2.commitN
	n2.mu.Unlock()

	if b1 == 0 || b2 == 0 {
		t.Fatalf("expected batched commit RPCs, got b1=%d b2=%d", b1, b2)
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
		ExecuteAndCommit(context.Background())

	committed, status := execution.Committed, execution.Status
	if !committed || status != StatusCommitted {
		t.Fatalf("unexpected transaction outcome: committed=%v status=%v", committed, status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.FlushPendingCommits(ctx); err != nil {
		t.Fatalf("flush pending commits: %v", err)
	}
	if got, _ := c.Read(context.Background(), 1, 4, 3); string(got) != "one" {
		t.Fatalf("node 1 write not applied after flush: %q", got)
	}
	if got, _ := c.Read(context.Background(), 2, 8, 3); string(got) != "two" {
		t.Fatalf("node 2 write not applied after flush: %q", got)
	}
}
