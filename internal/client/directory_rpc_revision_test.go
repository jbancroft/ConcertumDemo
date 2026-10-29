package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestDirectoryClientReconcilesExplicitUnknownOutcome(t *testing.T) {
	requested := sinfonia.DirectoryEntry{Primary: sinfonia.Endpoint{RPC: "new:1"}, Generation: 42}
	for _, test := range []struct {
		name     string
		resolved sinfonia.ResolveResponse
		wantErr  error
	}{
		{
			name:     "exact requested entry is success",
			resolved: sinfonia.ResolveResponse{MemID: 1, Primary: requested.Primary, Generation: requested.Generation, DirectoryRevision: 8},
		},
		{
			name:     "old selected revision stays unknown",
			resolved: sinfonia.ResolveResponse{MemID: 1, Primary: sinfonia.Endpoint{RPC: "old:1"}, Generation: 41, DirectoryRevision: 7},
			wantErr:  ErrDirectoryUpdateOutcomeUnknown,
		},
		{
			name:     "different selected revision is conflict",
			resolved: sinfonia.ResolveResponse{MemID: 1, Primary: sinfonia.Endpoint{RPC: "other:1"}, Generation: 43, DirectoryRevision: 9},
			wantErr:  ErrDirectoryUpdateConflict,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var observed sinfonia.UpdateEntryRequest
			client, closeServer := revisionTestDirectoryClient(t, func(req sinfonia.UpdateEntryRequest) sinfonia.UpdateEntryResponse {
				observed = req
				return sinfonia.UpdateEntryResponse{OutcomeUnknown: true, Revision: 8, CurrentGeneration: 42}
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
	client, closeServer := revisionTestDirectoryClient(t, func(sinfonia.UpdateEntryRequest) sinfonia.UpdateEntryResponse {
		return sinfonia.UpdateEntryResponse{Degraded: true, Revision: 12}
	}, sinfonia.ResolveResponse{})
	defer closeServer()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.UpdateEntryCASRevision(ctx, 1, sinfonia.DirectoryEntry{Primary: sinfonia.Endpoint{RPC: "new:1"}}, 0, 11)
	if !errors.Is(err, ErrDirectoryStoreDegraded) {
		t.Fatalf("err=%v want degraded", err)
	}
}

func TestDirectoryClientReconcilesTransportAmbiguity(t *testing.T) {
	requested := sinfonia.DirectoryEntry{Primary: sinfonia.Endpoint{RPC: "new:1"}, Generation: 42}
	for _, test := range []struct {
		name     string
		resolved sinfonia.ResolveResponse
		wantErr  error
	}{
		{"exact is success", sinfonia.ResolveResponse{Primary: requested.Primary, Generation: 42, DirectoryRevision: 8}, nil},
		{"old stays unknown", sinfonia.ResolveResponse{Primary: sinfonia.Endpoint{RPC: "old:1"}, Generation: 41, DirectoryRevision: 7}, ErrDirectoryUpdateOutcomeUnknown},
		{"different newer conflicts", sinfonia.ResolveResponse{Primary: sinfonia.Endpoint{RPC: "other:1"}, Generation: 43, DirectoryRevision: 9}, ErrDirectoryUpdateConflict},
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
	requested := sinfonia.DirectoryEntry{Primary: sinfonia.Endpoint{RPC: "new:1"}, Generation: 42}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, nil)
	rpcwire.RegisterTyped(server, sinfonia.MethodUpdateEntry, func(context.Context, sinfonia.UpdateEntryRequest) (sinfonia.UpdateEntryResponse, error) {
		time.Sleep(100 * time.Millisecond)
		return sinfonia.UpdateEntryResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(server, sinfonia.MethodResolve, func(context.Context, sinfonia.ResolveRequest) (sinfonia.ResolveResponse, error) {
		return sinfonia.ResolveResponse{Primary: requested.Primary, Generation: 42, DirectoryRevision: 8}, nil
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

func transportAmbiguityTestDirectoryClient(t *testing.T, resolved sinfonia.ResolveResponse) (*DirectoryClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, func(conn net.Conn) context.Context {
		return context.WithValue(context.Background(), revisionTestConnKey{}, conn)
	})
	rpcwire.RegisterTyped(server, sinfonia.MethodUpdateEntry, func(ctx context.Context, _ sinfonia.UpdateEntryRequest) (sinfonia.UpdateEntryResponse, error) {
		_ = ctx.Value(revisionTestConnKey{}).(net.Conn).Close()
		return sinfonia.UpdateEntryResponse{Ack: true}, nil
	})
	rpcwire.RegisterTyped(server, sinfonia.MethodResolve, func(context.Context, sinfonia.ResolveRequest) (sinfonia.ResolveResponse, error) {
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
	update func(sinfonia.UpdateEntryRequest) sinfonia.UpdateEntryResponse,
	resolved sinfonia.ResolveResponse,
) (*DirectoryClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcwire.NewServer(nil, nil)
	rpcwire.RegisterTyped(server, sinfonia.MethodUpdateEntry, func(_ context.Context, req sinfonia.UpdateEntryRequest) (sinfonia.UpdateEntryResponse, error) {
		return update(req), nil
	})
	rpcwire.RegisterTyped(server, sinfonia.MethodResolve, func(context.Context, sinfonia.ResolveRequest) (sinfonia.ResolveResponse, error) {
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
