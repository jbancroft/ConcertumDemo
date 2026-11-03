package sinfoniaclient

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func newTestParticipant(t testing.TB, id uint32, size int) *memnode.Participant {
	t.Helper()
	return openTestParticipant(t, id, size, t.TempDir())
}

func openTestParticipant(t testing.TB, id uint32, size int, dir string) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewParticipant(id, size, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := node.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	return node
}

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
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			coord := client.NewCoordinator(map[uint32]client.Participant{
				1: newTestParticipant(t, 1, 16), 2: newTestParticipant(t, 2, 16),
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

func TestTwoNodeAtomicCommit(t *testing.T) {
	n1 := newTestParticipant(t, 1, 16)
	n2 := newTestParticipant(t, 2, 16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	defer coord.Close()
	c := &Client{coordinator: coord}

	if err := c.Write(t.Context(), 1, 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(t.Context(), 2, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}

	execution := c.NewMiniTransaction().
		Compare(1, 0, []byte("a")).
		Compare(2, 0, []byte("x")).
		Write(1, 1, []byte("b")).
		Write(2, 1, []byte("y")).
		ExecuteAndCommit(t.Context())

	committed, status := execution.Committed, execution.Status

	if !committed || status != StatusCommitted {
		t.Fatalf("expected committed, got committed=%v status=%v", committed, status)
	}

	awaitValue(t, c, 1, 1, []byte("b"))
	awaitValue(t, c, 2, 1, []byte("y"))
}

func TestCompareAbortNoWritesApplied(t *testing.T) {
	n1 := newTestParticipant(t, 1, 16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	defer coord.Close()
	c := &Client{coordinator: coord}
	if err := c.Write(t.Context(), 1, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}

	execution := c.NewMiniTransaction().Compare(1, 0, []byte("nope")).Write(1, 0, []byte("new!")).ExecuteAndCommit(t.Context())

	committed, status := execution.Committed, execution.Status
	if committed {
		t.Fatalf("expected abort")
	}
	if status != StatusAborted {
		t.Fatalf("expected aborted status, got %v", status)
	}
	data, err := c.Read(t.Context(), 1, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "seed" {
		t.Fatalf("write applied on abort, got %q", string(data))
	}
}

func awaitValue(t *testing.T, c *Client, hostId uint32, addr uint64, expected []byte) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		data, err := c.Read(t.Context(), hostId, addr, uint32(len(expected)))
		if err == nil && string(data) == string(expected) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	data, _ := c.Read(t.Context(), hostId, addr, uint32(len(expected)))
	t.Fatalf("expected %q at mem %d addr %d, got %q", string(expected), hostId, addr, string(data))
}
