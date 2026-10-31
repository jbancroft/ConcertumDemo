package rpcwire

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestServeCancelsAndDrainsHandlers(t *testing.T) {
	type contextKey struct{}
	for _, test := range []struct {
		name        string
		connContext func(net.Conn) context.Context
		wantValue   any
	}{
		{name: "default context"},
		{
			name: "custom connection context",
			connContext: func(net.Conn) context.Context {
				return context.WithValue(context.Background(), contextKey{}, "connection value")
			},
			wantValue: "connection value",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testServeShutdown(t, test.connContext, contextKey{}, test.wantValue)
		})
	}
}

func testServeShutdown(t *testing.T, connContext func(net.Conn) context.Context, key, wantValue any) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := NewServer(connContext)
	entered := make(chan struct{})
	canceled := make(chan error, 1)
	release := make(chan struct{})
	defer close(release)
	server.Register("ping", func(context.Context, any) (any, error) { return "pong", nil })
	server.Register("block", func(ctx context.Context, _ any) (any, error) {
		if got := ctx.Value(key); got != wantValue {
			t.Errorf("handler context value = %v, want %v", got, wantValue)
		}
		close(entered)
		select {
		case <-ctx.Done():
			canceled <- ctx.Err()
		case <-release:
			return nil, nil // Let a failed test clean up an uncanceled handler.
		}
		// Cancellation requests shutdown; Serve must still wait for cleanup.
		<-release
		return "done", nil
	})
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	dial := func() net.Conn {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	idle := dial()
	if err := writeFrame(idle, Request{Method: "ping"}); err != nil {
		t.Fatal(err)
	}
	var reply Response
	if err := readFrame(idle, &reply); err != nil {
		t.Fatal(err)
	}
	active := dial()
	if err := writeFrame(active, Request{Method: "block"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case err := <-canceled:
		t.Fatalf("handler canceled before shutdown: %v", err)
	default:
	}
	_ = listener.Close()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not cancel the blocked handler")
	}
	if err := readFrame(idle, &reply); err == nil {
		t.Fatal("idle connection survived listener shutdown")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("idle connection was not closed")
	}
	select {
	case <-finished:
		t.Fatal("Serve returned before its active handler completed")
	default:
	}
	// Release via a value so the deferred close still handles an early failure.
	release <- struct{}{}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not finish after draining its handler")
	}
}
