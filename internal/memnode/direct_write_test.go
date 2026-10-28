package memnode_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/auth"
	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
)

func TestDurableDirectWriteRejectedWithoutLiveOrRestartMutation(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node")
	n, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	if err := n.Write(ctx, 4, []byte("raw")); !errors.Is(err, memnode.ErrDirectWriteRequiresMiniTransaction) {
		t.Fatalf("direct durable write error = %v, want %v", err, memnode.ErrDirectWriteRequiresMiniTransaction)
	}
	assertBytesAt(t, n, ctx, 4, []byte{0, 0, 0})
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.Write(ctx, 4, []byte("closed")); !errors.Is(err, memnode.ErrDirectWriteRequiresMiniTransaction) {
		t.Fatalf("post-close direct durable write error = %v, want %v", err, memnode.ErrDirectWriteRequiresMiniTransaction)
	}
	assertBytesAt(t, n, ctx, 4, []byte{0, 0, 0})

	restarted, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	assertBytesAt(t, restarted, ctx, 4, []byte{0, 0, 0})

	resp, err := restarted.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{
		RequestID:  "transactional-write",
		TID:        proto.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: 1},
		WriteItems: []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("mtx")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != proto.VoteOk {
		t.Fatalf("transactional durable write vote = %v, want %v", resp.Vote, proto.VoteOk)
	}
	assertBytesAt(t, restarted, ctx, 4, []byte("mtx"))
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	restartedAgain, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedAgain.Close() })
	if err := restartedAgain.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	assertBytesAt(t, restartedAgain, ctx, 4, []byte("mtx"))
}

func TestDurableDirectWritePreservesAuthorizationAndFencingPrecedence(t *testing.T) {
	ctx := context.Background()
	n, err := memnode.NewDurableParticipant(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ConfigureReplication(ctx, proto.ConfigureReplicationRequest{
		Role:                   proto.ReplicationRoleSingle,
		Generation:             42,
		FencingProtocolVersion: proto.ReplicationFencingProtocolVersion,
	}); err != nil {
		t.Fatal(err)
	}
	n.ConfigureSafety(memnode.SafetyConfig{Authorizer: &auth.Authorizer{}})

	if err := n.Write(proto.WithGeneration(ctx, 41), 0, []byte("x")); !errors.Is(err, memnode.ErrUnauthorized) {
		t.Fatalf("unauthenticated write error = %v, want %v", err, memnode.ErrUnauthorized)
	}
	clientCtx := auth.WithIdentity(ctx, auth.Identity{Name: "client", Role: auth.RoleClient})
	if err := n.Write(proto.WithGeneration(clientCtx, 41), 0, []byte("x")); !errors.Is(err, memnode.ErrStaleGeneration) {
		t.Fatalf("stale write error = %v, want %v", err, memnode.ErrStaleGeneration)
	}
	if err := n.Write(proto.WithGeneration(clientCtx, 42), 0, []byte("x")); !errors.Is(err, memnode.ErrDirectWriteRequiresMiniTransaction) {
		t.Fatalf("current-generation write error = %v, want %v", err, memnode.ErrDirectWriteRequiresMiniTransaction)
	}
	assertBytesAt(t, n, proto.WithGeneration(clientCtx, 42), 0, []byte{0})
}

func TestInMemoryDirectWriteRemainsSupported(t *testing.T) {
	ctx := context.Background()
	n := memnode.NewParticipant(16)
	if err := n.Write(ctx, 3, []byte("ram")); err != nil {
		t.Fatal(err)
	}
	assertBytesAt(t, n, ctx, 3, []byte("ram"))
}

func TestDurableDirectWriteRejectedInEveryReplicationMode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*memnode.Participant)
	}{
		{name: "single", configure: func(*memnode.Participant) {}},
		{name: "primary", configure: func(n *memnode.Participant) { n.ConfigureAsPrimary(2, nil) }},
		{name: "replica", configure: func(n *memnode.Participant) { n.ConfigureAsReplica(2) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := memnode.NewDurableParticipant(1, 16, filepath.Join(t.TempDir(), tc.name))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = n.Close() })
			if err := n.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			tc.configure(n)
			if err := n.Write(context.Background(), 0, []byte("x")); !errors.Is(err, memnode.ErrDirectWriteRequiresMiniTransaction) {
				t.Fatalf("direct write error = %v, want %v", err, memnode.ErrDirectWriteRequiresMiniTransaction)
			}
		})
	}
}

func TestDurableDirectWriteRPCSurfacesRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := memnode.NewDurableParticipant(1, 16, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	rpcNode := client.NewRPCParticipant(startMemnodeRPC(t, n), nil, time.Second)

	err = rpcNode.Write(ctx, 0, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), memnode.ErrDirectWriteRequiresMiniTransaction.Error()) {
		t.Fatalf("RPC direct write error = %v, want %v", err, memnode.ErrDirectWriteRequiresMiniTransaction)
	}
	assertBytesAt(t, n, ctx, 0, []byte{0})
}

func assertBytesAt(t *testing.T, n *memnode.Participant, ctx context.Context, addr uint64, want []byte) {
	t.Helper()
	got, err := n.Read(ctx, addr, uint32(len(want)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("data at %d = %v, want %v", addr, got, want)
	}
}
