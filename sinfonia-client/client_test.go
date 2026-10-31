package sinfoniaclient

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestClientCloseReleasesWorkers(t *testing.T) {
	workerCount := func() int {
		stacks := make([]byte, 1<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		return strings.Count(string(stacks), "(*rpcWorker).runPrepare(") +
			strings.Count(string(stacks), "(*rpcWorker).runCommitBatcher(")
	}
	before := workerCount()
	for range 10 {
		c := NewClient(map[uint32]NodeConfig{1: {RPCAddress: "unused"}})
		for range 2 {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	deadline := time.Now().Add(time.Second)
	for workerCount() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if after := workerCount(); after > before {
		t.Fatalf("workers retained after Close: before=%d after=%d", before, after)
	}
}

type backoffFunc func(context.Context, int)

func (f backoffFunc) Wait(ctx context.Context, attempt int) { f(ctx, attempt) }

func TestExecuteAndCommitAcrossTwoNodes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		compare   string
		committed bool
		status    Status
		vote      sinfonia.Vote
	}{
		{name: "commit", compare: "a", committed: true, status: StatusCommitted, vote: sinfonia.VoteOk},
		{name: "compare abort", compare: "stale", status: StatusAborted, vote: sinfonia.VoteBadCmp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			coord := client.NewCoordinator(map[uint32]client.Participant{
				1: memnode.NewParticipant(16), 2: memnode.NewParticipant(16),
			})
			defer coord.Close()
			c := &Client{coordinator: coord}
			for i, value := range []string{"a", "b"} {
				if err := c.Write(ctx, uint32(i+1), 0, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			result := c.ExecuteAndCommit(ctx, []CompareItem{{HostId: 1, Address: 0, Data: []byte(tc.compare)}},
				[]ReadItem{{HostId: 1, Address: 0, Length: 1}, {HostId: 2, Address: 0, Length: 1}},
				[]WriteItem{{HostId: 1, Address: 0, Data: []byte("x")}, {HostId: 2, Address: 0, Data: []byte("y")}},
			)
			if result.Committed != tc.committed || result.Status != tc.status || result.UnknownOutcome {
				t.Fatalf("unexpected transaction result: %+v", result)
			}
			if len(result.ParticipantVotes) != 2 || result.ParticipantVotes[1] != int(tc.vote) || result.ParticipantVotes[2] != int(sinfonia.VoteOk) {
				t.Fatalf("unexpected participant votes: %v", result.ParticipantVotes)
			}
			if tc.committed {
				for i, want := range []string{"a", "b"} {
					key := ReadKey{HostId: uint32(i + 1), Address: 0, Length: 1}
					if got := string(result.ReadResults[key]); got != want {
						t.Errorf("read result for node %d = %q, want %q", key.HostId, got, want)
					}
				}
			} else if len(result.ReadResults) != 0 {
				t.Fatalf("aborted transaction returned reads: %v", result.ReadResults)
			}
			if err := c.FlushPendingCommits(ctx); err != nil {
				t.Fatal(err)
			}
			values := []string{"a", "b"}
			if tc.committed {
				values = []string{"x", "y"}
			}
			for i, want := range values {
				got, err := c.Read(ctx, uint32(i+1), 0, 1)
				if err != nil || string(got) != want {
					t.Errorf("node %d = %q, want %q; error = %v", i+1, got, want, err)
				}
			}
		})
	}
}

type lostOnePCReplyParticipant struct {
	*memnode.Participant
}

func (n *lostOnePCReplyParticipant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	resp, err := n.Participant.ExecCommit1PC(ctx, req)
	if err != nil || len(req.WriteItems) == 0 {
		return resp, err
	}
	return sinfonia.ExecCommit1PCResponse{}, errors.New("lost commit reply")
}

func TestExecuteAndCommitResolvesLostReply(t *testing.T) {
	ctx := context.Background()
	n := &lostOnePCReplyParticipant{Participant: memnode.NewParticipant(16)}
	c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: n})}
	defer c.Close()
	result := c.ExecuteAndCommit(ctx, nil, nil, []WriteItem{{HostId: 1, Address: 0, Data: []byte("x")}})
	if !result.Committed || result.UnknownOutcome || result.Status != StatusCommitted {
		t.Fatalf("lost reply should resolve to committed: %+v", result)
	}
	if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "x" {
		t.Fatalf("write should have applied before the reply was lost: data=%q error=%v", got, err)
	}
}

func TestTwoNodeAtomicCommit(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	n2 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	if err := c.Write(context.Background(), 1, 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(context.Background(), 2, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}

	execution := c.NewMiniTransaction().
		Compare(1, 0, []byte("a")).
		Compare(2, 0, []byte("x")).
		Write(1, 1, []byte("b")).
		Write(2, 1, []byte("y")).
		ExecuteAndCommit(context.Background())

	committed, status := execution.Committed, execution.Status

	if !committed || status != StatusCommitted {
		t.Fatalf("expected committed, got committed=%v status=%v", committed, status)
	}

	awaitValue(t, c, 1, 1, []byte("b"))
	awaitValue(t, c, 2, 1, []byte("y"))
}

func TestCompareAbortNoWritesApplied(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	defer coord.Close()
	c := &Client{coordinator: coord}
	if err := c.Write(context.Background(), 1, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}

	execution := c.NewMiniTransaction().Compare(1, 0, []byte("nope")).Write(1, 0, []byte("new!")).ExecuteAndCommit(context.Background())

	committed, status := execution.Committed, execution.Status
	if committed {
		t.Fatalf("expected abort")
	}
	if status != StatusAborted {
		t.Fatalf("expected aborted status, got %v", status)
	}
	data, err := c.Read(context.Background(), 1, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "seed" {
		t.Fatalf("write applied on abort, got %q", string(data))
	}
}

func TestBadLockRetryThenSuccess(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	n2 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	if err := c.Write(context.Background(), 1, 0, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(context.Background(), 2, 0, []byte("xy")); err != nil {
		t.Fatal(err)
	}

	release := n1.HoldWriteLockForTest(0, 2)
	released := false
	coord.SetBackoffForTest(backoffFunc(func(_ context.Context, _ int) {
		if !released {
			release()
			released = true
		}
		time.Sleep(5 * time.Millisecond)
	}))
	coord.SetMaxRetriesForTest(3)

	execution := c.NewMiniTransaction().
		Compare(1, 0, []byte("ab")).
		Compare(2, 0, []byte("xy")).
		Write(1, 0, []byte("cd")).
		Write(2, 0, []byte("zw")).
		ExecuteAndCommit(context.Background())

	committed, status := execution.Committed, execution.Status

	if !committed || status != StatusCommitted {
		t.Fatalf("expected commit after retry, got committed=%v status=%v", committed, status)
	}
	awaitValue(t, c, 1, 0, []byte("cd"))
	awaitValue(t, c, 2, 0, []byte("zw"))
}

func awaitValue(t *testing.T, c *Client, hostId uint32, addr uint64, expected []byte) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		data, err := c.Read(context.Background(), hostId, addr, uint32(len(expected)))
		if err == nil && string(data) == string(expected) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	data, _ := c.Read(context.Background(), hostId, addr, uint32(len(expected)))
	t.Fatalf("expected %q at mem %d addr %d, got %q", string(expected), hostId, addr, string(data))
}
