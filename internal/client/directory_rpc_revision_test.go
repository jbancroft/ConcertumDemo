package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestDirectoryClientReconcilesExplicitUnknownOutcome(t *testing.T) {
	requested := proto.DirectoryEntry{Primary: proto.Endpoint{RPC: "new:1"}, Generation: 42}
	for _, test := range []struct {
		name     string
		resolved proto.ResolveResponse
		wantErr  error
	}{
		{
			name:     "exact requested entry is success",
			resolved: proto.ResolveResponse{MemID: 1, Primary: requested.Primary, Generation: requested.Generation, DirectoryRevision: 8},
		},
		{
			name:     "old selected revision stays unknown",
			resolved: proto.ResolveResponse{MemID: 1, Primary: proto.Endpoint{RPC: "old:1"}, Generation: 41, DirectoryRevision: 7},
			wantErr:  ErrDirectoryUpdateOutcomeUnknown,
		},
		{
			name:     "different selected revision is conflict",
			resolved: proto.ResolveResponse{MemID: 1, Primary: proto.Endpoint{RPC: "other:1"}, Generation: 43, DirectoryRevision: 9},
			wantErr:  ErrDirectoryUpdateConflict,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var observed proto.UpdateEntryRequest
			client, closeServer := revisionTestDirectoryClient(t, func(req proto.UpdateEntryRequest) proto.UpdateEntryResponse {
				observed = req
				return proto.UpdateEntryResponse{OutcomeUnknown: true, Revision: 8, CurrentGeneration: 42}
			}, test.resolved)
			defer closeServer()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := client.UpdateEntryCASRevision(ctx, 1, requested, 41, 7)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("err=%v want errors.Is(_, %v)", err, test.wantErr)
			}
			if observed.ExpectedGeneration != 41 || observed.ExpectedRevision != 7 || !observed.ExpectedRevisionSet {
				t.Fatalf("observed guards=%+v", observed)
			}
		})
	}
}

func TestDirectoryClientReturnsExplicitDegradedOutcome(t *testing.T) {
	client, closeServer := revisionTestDirectoryClient(t, func(proto.UpdateEntryRequest) proto.UpdateEntryResponse {
		return proto.UpdateEntryResponse{Degraded: true, Revision: 12}
	}, proto.ResolveResponse{})
	defer closeServer()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.UpdateEntryCASRevision(ctx, 1, proto.DirectoryEntry{Primary: proto.Endpoint{RPC: "new:1"}}, 0, 11)
	if !errors.Is(err, ErrDirectoryStoreDegraded) {
		t.Fatalf("err=%v want degraded", err)
	}
}

func TestDirectoryClientReconcilesTransportAmbiguity(t *testing.T) {
	requested := proto.DirectoryEntry{Primary: proto.Endpoint{RPC: "new:1"}, Generation: 42}
	for _, test := range []struct {
		name     string
		resolved proto.ResolveResponse
		wantErr  error
	}{
		{"exact is success", proto.ResolveResponse{Primary: requested.Primary, Generation: 42, DirectoryRevision: 8}, nil},
		{"old stays unknown", proto.ResolveResponse{Primary: proto.Endpoint{RPC: "old:1"}, Generation: 41, DirectoryRevision: 7}, ErrDirectoryUpdateOutcomeUnknown},
		{"different newer conflicts", proto.ResolveResponse{Primary: proto.Endpoint{RPC: "other:1"}, Generation: 43, DirectoryRevision: 9}, ErrDirectoryUpdateConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, closeServer := transportAmbiguityTestDirectoryClient(t, test.resolved)
			defer closeServer()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := client.UpdateEntryCASRevision(ctx, 1, requested, 41, 7)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("err=%v want errors.Is(_, %v)", err, test.wantErr)
			}
		})
	}
}

func TestDirectoryClientReconcilesTransportTimeoutAfterCallerDeadline(t *testing.T) {
	requested := proto.DirectoryEntry{Primary: proto.Endpoint{RPC: "new:1"}, Generation: 42}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, nil)
	rpcwire.RegisterTyped(server, proto.MethodUpdateEntry, func(context.Context, proto.UpdateEntryRequest) (proto.UpdateEntryResponse, error) {
		time.Sleep(100 * time.Millisecond)
		return proto.UpdateEntryResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(server, proto.MethodResolve, func(context.Context, proto.ResolveRequest) (proto.ResolveResponse, error) {
		return proto.ResolveResponse{Primary: requested.Primary, Generation: 42, DirectoryRevision: 8}, nil
	})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	client := NewDirectoryClient(listener.Addr().String(), nil)
	client.SetTimeout(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err = client.UpdateEntryCASRevision(ctx, 1, requested, 41, 7)
	cancel()
	_ = client.rpc.Close()
	_ = listener.Close()
	<-done
	if err != nil {
		t.Fatalf("exact selected entry after timeout should reconcile as success: %v", err)
	}
}

type revisionTestConnKey struct{}

func transportAmbiguityTestDirectoryClient(t *testing.T, resolved proto.ResolveResponse) (*DirectoryClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, func(conn net.Conn) context.Context {
		return context.WithValue(context.Background(), revisionTestConnKey{}, conn)
	})
	rpcwire.RegisterTyped(server, proto.MethodUpdateEntry, func(ctx context.Context, _ proto.UpdateEntryRequest) (proto.UpdateEntryResponse, error) {
		_ = ctx.Value(revisionTestConnKey{}).(net.Conn).Close()
		return proto.UpdateEntryResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(server, proto.MethodResolve, func(context.Context, proto.ResolveRequest) (proto.ResolveResponse, error) {
		return resolved, nil
	})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	client := NewDirectoryClient(listener.Addr().String(), nil)
	return client, func() {
		_ = client.rpc.Close()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("rpc server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("rpc server did not stop")
		}
	}
}

func revisionTestDirectoryClient(
	t *testing.T,
	update func(proto.UpdateEntryRequest) proto.UpdateEntryResponse,
	resolved proto.ResolveResponse,
) (*DirectoryClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, nil)
	rpcwire.RegisterTyped(server, proto.MethodUpdateEntry, func(_ context.Context, req proto.UpdateEntryRequest) (proto.UpdateEntryResponse, error) {
		return update(req), nil
	})
	rpcwire.RegisterTyped(server, proto.MethodResolve, func(context.Context, proto.ResolveRequest) (proto.ResolveResponse, error) {
		return resolved, nil
	})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	client := NewDirectoryClient(listener.Addr().String(), nil)
	return client, func() {
		_ = client.rpc.Close()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("rpc server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("rpc server did not stop")
		}
	}
}
