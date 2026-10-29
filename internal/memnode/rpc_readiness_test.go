package memnode_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestRPCRejectsClientWorkUntilRecoveryCompletes(t *testing.T) {
	node, err := memnode.NewDurableParticipant(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = node.RPCServer().Serve(listener) }()

	client := rpcwire.NewClient(listener.Addr().String(), rpcwire.ClientOptions{DialTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var read sinfonia.ReadResponse
	err = client.Call(ctx, sinfonia.MethodRead, sinfonia.ReadRequest{Addr: 0, Len: 1}, &read)
	if err == nil || !strings.Contains(err.Error(), memnode.ErrRecoveryPending.Error()) {
		t.Fatalf("Read before recovery error = %v, want ErrRecoveryPending", err)
	}

	var entries sinfonia.ListRedoEntriesResponse
	if err := client.Call(ctx, sinfonia.MethodListRedoEntries, sinfonia.ListRedoEntriesRequest{}, &entries); err != nil {
		t.Fatalf("recovery-safe RPC failed while unready: %v", err)
	}

	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(ctx, sinfonia.MethodRead, sinfonia.ReadRequest{Addr: 0, Len: 1}, &read); err != nil {
		t.Fatalf("Read after recovery failed: %v", err)
	}
}
