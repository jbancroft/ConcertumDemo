package sinfoniaclient

import (
	"context"
	"errors"
	"math"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func accessClient(t *testing.T, node *memnode.Participant) (*Client, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- node.RPCServer().Serve(listener) }()
	stop := sync.OnceFunc(func() {
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Errorf("stop RPC server: %v", err)
		}
	})
	t.Cleanup(stop)
	c := NewClient(map[uint32]NodeConfig{1: {RPCAddress: listener.Addr().String()}})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.SetRetryPolicy(0, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return c, stop
}

func durableAccessNode(t *testing.T, dir string) *memnode.Participant {
	t.Helper()
	n, err := memnode.NewDurableParticipant(1, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestClientReadAndWriteRespectPreparedLocks(t *testing.T) {
	for _, mode := range []string{"RAM", "durable"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			n := memnode.NewParticipantWithId(1, 16)
			if mode == "durable" {
				n = durableAccessNode(t, t.TempDir())
			}
			c, _ := accessClient(t, n)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			prepared, err := n.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID: tid, Participants: []uint32{1}, HasWrites: true,
				CmpItems:   []sinfonia.CmpItem{{Addr: 0, Data: []byte{0, 0}}},
				WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: []byte{1}}},
			})
			if err != nil || prepared.Vote != sinfonia.VoteOk {
				t.Fatalf("prepare = %+v, %v", prepared, err)
			}
			if data, err := c.Read(ctx, 1, 0, 1); !errors.Is(err, ErrTransactionAborted) || data != nil {
				t.Fatalf("read bypassed prepared write: %v, %v", data, err)
			}
			// The comparison's shared lock permits reads but must reject writes.
			if data, err := c.Read(ctx, 1, 1, 1); err != nil || len(data) != 1 || data[0] != 0 {
				t.Fatalf("shared read = %v, %v", data, err)
			}
			if err := c.Write(ctx, 1, 1, []byte{5}); !errors.Is(err, ErrTransactionAborted) {
				t.Fatalf("write bypassed prepared comparison: %v", err)
			}
			if err := c.Write(ctx, 1, 4, []byte{9}); err != nil {
				t.Fatalf("unrelated write was blocked: %v", err)
			}
			if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
				t.Fatalf("commit = %+v, %v", resp, err)
			}
			data, err := c.Read(ctx, 1, 0, 2)
			if err != nil || len(data) != 2 || data[0] != 1 || data[1] != 0 {
				t.Fatalf("committed bytes = %v, %v; want [1 0]", data, err)
			}
			if err := c.Write(ctx, 1, 1, []byte{5}); err != nil {
				t.Fatalf("write after lock release: %v", err)
			}
		})
	}
}

func TestClientWritePersists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := filepath.Join(t.TempDir(), "node")
	n := durableAccessNode(t, dir)

	c, stop := accessClient(t, n)
	if err := c.Write(ctx, 1, 4, []byte("saved")); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := durableAccessNode(t, dir)
	c, _ = accessClient(t, reopened)
	if data, err := c.Read(ctx, 1, 4, 5); err != nil || string(data) != "saved" {
		t.Fatalf("recovered write = %q, %v", data, err)
	}
}

func TestClientWriteReportsUnknownAfterServerCommitError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	n := durableAccessNode(t, dir)
	var attempts atomic.Int32
	n.SetResponseBoundaryHookForTest(func(string, sinfonia.TransactionID) error {
		attempts.Add(1)
		return errors.New("injected failure after commit decision")
	})
	c, stop := accessClient(t, n)
	if err := c.SetRetryPolicy(3, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	err := c.Write(ctx, 1, 4, []byte("saved"))
	if !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("ambiguous write must report an unknown outcome: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("ambiguous write was retried: %d attempts", attempts.Load())
	}
	stop()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := durableAccessNode(t, dir)
	c, _ = accessClient(t, reopened)
	if data, err := c.Read(ctx, 1, 4, 5); err != nil || string(data) != "saved" {
		t.Fatalf("committed write did not recover: %q, %v", data, err)
	}
}

func TestClientAccessRejectsInvalidRanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _ := accessClient(t, memnode.NewParticipantWithId(1, 16))
	for _, addr := range []uint64{15, math.MaxUint64} {
		if data, err := c.Read(ctx, 1, addr, 2); err == nil || data != nil {
			t.Fatalf("read at %d = %v, %v", addr, data, err)
		}
		if err := c.Write(ctx, 1, addr, []byte{1, 2}); err == nil {
			t.Fatalf("write at %d succeeded", addr)
		}
	}
	if err := c.Write(ctx, 1, 15, []byte{7}); err != nil {
		t.Fatal(err)
	}
	if data, err := c.Read(ctx, 1, 15, 1); err != nil || len(data) != 1 || data[0] != 7 {
		t.Fatalf("last byte = %v, %v", data, err)
	}
}
