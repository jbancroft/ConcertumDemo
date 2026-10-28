package memnode_test

import (
	"net"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
)

// Extracted from the excluded manager integration tests.
func startMemnodeRPC(t *testing.T, node *memnode.Participant) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = node.RPCServer().Serve(listener) }()
	return listener.Addr().String()
}
