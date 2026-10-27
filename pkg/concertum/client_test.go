package concertum_test

import (
	"context"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/client"
	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/pkg/concertum"
)

type backoffFunc func(context.Context, int)

func (f backoffFunc) Wait(ctx context.Context, attempt int) { f(ctx, attempt) }

func TestTwoNodeAtomicCommit(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	n2 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1, 2: n2})
	c := concertum.NewClient(newCoordinatorBackend(coord))

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

	if !committed || status != concertum.StatusCommitted {
		t.Fatalf("expected committed, got committed=%v status=%v", committed, status)
	}

	awaitValue(t, c, 1, 1, []byte("b"))
	awaitValue(t, c, 2, 1, []byte("y"))
}

func TestCompareAbortNoWritesApplied(t *testing.T) {
	n1 := memnode.NewParticipant(16)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	c := concertum.NewClient(newCoordinatorBackend(coord))
	if err := c.Write(context.Background(), 1, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}

	committed, _, status := c.NewMiniTransaction().Compare(1, 0, []byte("nope")).Write(1, 0, []byte("new!")).ExecuteAndCommit(context.Background())
	if committed {
		t.Fatalf("expected abort")
	}
	if status != concertum.StatusAborted {
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
	c := concertum.NewClient(newCoordinatorBackend(coord))

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

	if !committed || status != concertum.StatusCommitted {
		t.Fatalf("expected commit after retry, got committed=%v status=%v", committed, status)
	}
	awaitValue(t, c, 1, 0, []byte("cd"))
	awaitValue(t, c, 2, 0, []byte("zw"))
}

func TestMTxWithClassExposesClassStats(t *testing.T) {
	n1 := memnode.NewParticipant(32)
	coord := client.NewCoordinator(map[uint32]client.Participant{1: n1})
	c := concertum.NewClient(newCoordinatorBackend(coord))
	committed, _, status := c.NewMiniTransaction().WithClass("inventory").Write(1, 0, []byte("z")).ExecuteAndCommit(context.Background())
	if !committed || status != concertum.StatusCommitted {
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
	c := concertum.NewClient(newCoordinatorBackend(coord))
	committed, _, status := c.NewMiniTransaction().Write(1, 0, []byte("z")).ExecuteAndCommit(context.Background())
	if !committed || status != concertum.StatusCommitted {
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

func awaitValue(t *testing.T, c *concertum.Client, hostId uint32, addr uint64, expected []byte) {
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
