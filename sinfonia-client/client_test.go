package sinfoniaclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

type backoffFunc func(context.Context, int)

func (f backoffFunc) Wait(ctx context.Context, attempt int) { f(ctx, attempt) }

func TestExecuteAndCommitAcrossTwoNodes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		compare   string
		committed bool
		status    sinfoniaclient.Status
		vote      proto.Vote
	}{
		{name: "commit", compare: "a", committed: true, status: sinfoniaclient.StatusCommitted, vote: proto.VoteOk},
		{name: "compare abort", compare: "stale", status: sinfoniaclient.StatusAborted, vote: proto.VoteBadCmp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			coord := client.NewCoordinator(map[uint32]client.Participant{
				1: memnode.NewParticipant(16), 2: memnode.NewParticipant(16),
			})
			c := sinfoniaclient.NewClient(coord)
			for i, value := range []string{"a", "b"} {
				if err := c.Write(ctx, uint32(i+1), 0, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			result := c.ExecuteAndCommit(ctx, "direct",
				[]sinfoniaclient.CompareItem{{HostId: 1, Address: 0, Data: []byte(tc.compare)}},
				[]sinfoniaclient.ReadItem{{HostId: 1, Address: 0, Length: 1}, {HostId: 2, Address: 0, Length: 1}},
				[]sinfoniaclient.WriteItem{{HostId: 1, Address: 0, Data: []byte("x")}, {HostId: 2, Address: 0, Data: []byte("y")}},
			)
			if result.Committed != tc.committed || result.Status != tc.status || result.UnknownOutcome {
				t.Fatalf("unexpected transaction result: %+v", result)
			}
			if len(result.ParticipantVotes) != 2 || result.ParticipantVotes[1] != int(tc.vote) || result.ParticipantVotes[2] != int(proto.VoteOk) {
				t.Fatalf("unexpected participant votes: %v", result.ParticipantVotes)
			}
			if tc.committed {
				for i, want := range []string{"a", "b"} {
					key := sinfoniaclient.ReadKey{HostId: uint32(i + 1), Address: 0, Length: 1}
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

func (n *lostOnePCReplyParticipant) ExecCommit1PC(ctx context.Context, req proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error) {
	resp, err := n.Participant.ExecCommit1PC(ctx, req)
	if err != nil {
		return resp, err
	}
	return proto.ExecCommit1PCResponse{}, errors.New("lost commit reply")
}

func TestExecuteAndCommitReportsUnknownAfterLostReply(t *testing.T) {
	ctx := context.Background()
	n := &lostOnePCReplyParticipant{Participant: memnode.NewParticipant(16)}
	c := sinfoniaclient.NewClient(client.NewCoordinator(map[uint32]client.Participant{1: n}))
	result := c.ExecuteAndCommit(ctx, "", nil, nil, []sinfoniaclient.WriteItem{{HostId: 1, Address: 0, Data: []byte("x")}})
	if result.Committed || !result.UnknownOutcome || result.Status != sinfoniaclient.StatusUnknown {
		t.Fatalf("lost reply should produce an unknown outcome: %+v", result)
	}
	if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "x" {
		t.Fatalf("write should have applied before the reply was lost: data=%q error=%v", got, err)
	}
}

func TestTwoNodeAtomicCommit(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	n2 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	c := sinfoniaclient.NewClient(coord)

	if err := c.Write(context.Background(), 1, 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(context.Background(), 2, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}

	committed, _, status := c.NewMiniTransaction().
		Compare(1, 0, []byte("a")).
		Compare(2, 0, []byte("x")).
		Write(1, 1, []byte("b")).
		Write(2, 1, []byte("y")).
		ExecuteAndCommit(context.Background())

	if !committed || status != sinfoniaclient.StatusCommitted {
		t.Fatalf("expected committed, got committed=%v status=%v", committed, status)
	}

	awaitValue(t, c, 1, 1, []byte("b"))
	awaitValue(t, c, 2, 1, []byte("y"))
}

func TestCompareAbortNoWritesApplied(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	c := sinfoniaclient.NewClient(coord)
	if err := c.Write(context.Background(), 1, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}

	committed, _, status := c.NewMiniTransaction().Compare(1, 0, []byte("nope")).Write(1, 0, []byte("new!")).ExecuteAndCommit(context.Background())
	if committed {
		t.Fatalf("expected abort")
	}
	if status != sinfoniaclient.StatusAborted {
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
	c := sinfoniaclient.NewClient(coord)

	if err := c.Write(context.Background(), 1, 0, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(context.Background(), 2, 0, []byte("xy")); err != nil {
		t.Fatal(err)
	}

	committed, _, status := c.NewMiniTransaction().
		Compare(1, 0, []byte("ab")).
		Compare(2, 0, []byte("xy")).
		Write(1, 0, []byte("cd")).
		Write(2, 0, []byte("zw")).
		ExecuteAndCommit(context.Background())

	if !committed || status != sinfoniaclient.StatusCommitted {
		t.Fatalf("expected commit after retry, got committed=%v status=%v", committed, status)
	}
	awaitValue(t, c, 1, 0, []byte("cd"))
	awaitValue(t, c, 2, 0, []byte("zw"))
}

func TestMTxWithClassExposesClassStats(t *testing.T) {
	n1 := memnode.NewParticipant(32)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	c := sinfoniaclient.NewClient(coord)
	committed, _, status := c.NewMiniTransaction().WithClass("inventory").Write(1, 0, []byte("z")).ExecuteAndCommit(context.Background())
	if !committed || status != sinfoniaclient.StatusCommitted {
		t.Fatalf("expected committed inventory transaction")
	}
	stats, err := c.GetStats(1)
	if err != nil {
		t.Fatal(err)
	}
	inv, ok := stats.ByClass["inventory"]
	if !ok {
		t.Fatalf("expected inventory class stats, got %#v", stats.ByClass)
	}
	if inv.Committed != 1 || inv.Total != 1 {
		t.Fatalf("unexpected inventory stats: %+v", inv)
	}
}

func TestMTxWithoutClassFallsBackToUnclassified(t *testing.T) {
	n1 := memnode.NewParticipant(32)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	c := sinfoniaclient.NewClient(coord)
	committed, _, status := c.NewMiniTransaction().Write(1, 0, []byte("z")).ExecuteAndCommit(context.Background())
	if !committed || status != sinfoniaclient.StatusCommitted {
		t.Fatalf("expected committed unclassified transaction")
	}
	stats, err := c.GetStats(1)
	if err != nil {
		t.Fatal(err)
	}
	unclassified, ok := stats.ByClass["__unclassified__"]
	if !ok {
		t.Fatalf("expected __unclassified__ class stats, got %#v", stats.ByClass)
	}
	if unclassified.Total != 1 || unclassified.Committed != 1 {
		t.Fatalf("unexpected __unclassified__ stats: %+v", unclassified)
	}
}

func awaitValue(t *testing.T, c *sinfoniaclient.Client, hostId uint32, addr uint64, expected []byte) {
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
