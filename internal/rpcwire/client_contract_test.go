package rpcwire

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/proto"
)

func TestClientNormalizesServerErrors(t *testing.T) {
	server := NewServer(nil, nil)
	server.Register("test.fail", func(context.Context, any) (any, error) {
		return nil, errors.New("request rejected")
	})
	RegisterTyped(server, "test.typed", func(context.Context, proto.ReadRequest) (proto.ReadResponse, error) {
		return proto.ReadResponse{}, nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()

	client := NewClient(listener.Addr().String(), ClientOptions{DialTimeout: time.Second})
	defer client.Close()
	tests := []struct {
		name        string
		method      string
		request     any
		wantMessage string
		wantParts   []string
	}{
		{
			name:        "handler rejection",
			method:      "test.fail",
			request:     proto.ReadRequest{},
			wantMessage: "request rejected",
		},
		{
			name:        "unknown method",
			method:      "test.missing",
			request:     proto.ReadRequest{},
			wantMessage: `unknown rpc method "test.missing"`,
		},
		{
			name:      "unexpected request type",
			method:    "test.typed",
			request:   proto.WriteRequest{},
			wantParts: []string{"unexpected request type", "test.typed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			out := proto.ReadResponse{Data: []byte("unchanged")}
			err := client.Call(ctx, tt.method, tt.request, &out)
			if err == nil {
				t.Fatal("Call succeeded, want remote error")
			}
			var remote *RemoteError
			if !errors.As(err, &remote) {
				t.Fatalf("Call error type = %T, want *RemoteError", err)
			}
			if remote.Method != tt.method {
				t.Fatalf("RemoteError.Method = %q, want %q", remote.Method, tt.method)
			}
			if tt.wantMessage != "" && remote.Message != tt.wantMessage {
				t.Fatalf("RemoteError.Message = %q, want %q", remote.Message, tt.wantMessage)
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(remote.Message, part) {
					t.Fatalf("RemoteError.Message = %q, want substring %q", remote.Message, part)
				}
			}
			if got, want := err.Error(), tt.method+": "+remote.Message; got != want {
				t.Fatalf("error text = %q, want %q", got, want)
			}
			if got := string(out.Data); got != "unchanged" {
				t.Fatalf("response changed on remote error: %q", got)
			}
		})
	}
}

func TestClientRejectsUnexpectedSuccessBody(t *testing.T) {
	server := NewServer(nil, nil)
	server.Register("test.wrong_response", func(context.Context, any) (any, error) {
		return proto.WriteResponse{Ack: true}, nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()

	client := NewClient(listener.Addr().String(), ClientOptions{DialTimeout: time.Second})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := proto.ReadResponse{Data: []byte("unchanged")}
	err = client.Call(ctx, "test.wrong_response", proto.ReadRequest{}, &out)
	if err == nil || !strings.Contains(err.Error(), "unexpected response type") {
		t.Fatalf("Call error = %v, want unexpected response type", err)
	}
	var remote *RemoteError
	if errors.As(err, &remote) {
		t.Fatalf("local response error was normalized as remote: %#v", remote)
	}
	if got := string(out.Data); got != "unchanged" {
		t.Fatalf("response changed on type mismatch: %q", got)
	}
}

func TestClientRetiresConnectionAfterMalformedResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := &Client{
		opts: ClientOptions{MaxIdle: 1},
		idle: []net.Conn{clientConn},
	}
	defer client.Close()

	serverResult := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		var request Request
		if err := readFrame(serverConn, &request); err != nil {
			serverResult <- err
			return
		}
		_, err := serverConn.Write(testFrame([]byte{0xff}))
		serverResult <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := proto.ReadResponse{Data: []byte("unchanged")}
	err := client.Call(ctx, "test.read", proto.ReadRequest{}, &out)
	if err == nil {
		t.Fatal("Call accepted malformed response")
	}
	var remote *RemoteError
	if errors.As(err, &remote) {
		t.Fatalf("malformed response was normalized as remote: %#v", remote)
	}
	if got := string(out.Data); got != "unchanged" {
		t.Fatalf("response changed after malformed frame: %q", got)
	}
	if len(client.idle) != 0 {
		t.Fatalf("malformed connection returned to idle pool: %d", len(client.idle))
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("serve malformed response: %v", err)
	}
}

func TestServerDoesNotDispatchMalformedRequest(t *testing.T) {
	server := NewServer(nil, nil)
	var called atomic.Bool
	server.Register("test.read", func(context.Context, any) (any, error) {
		called.Store(true)
		return proto.ReadResponse{}, nil
	})

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go server.serveConn(serverConn)
	if _, err := clientConn.Write(testFrame([]byte{0xff})); err != nil {
		t.Fatalf("write malformed request: %v", err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var response [1]byte
	if n, err := clientConn.Read(response[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("malformed request response = (%d, %v), want (0, EOF)", n, err)
	}
	if called.Load() {
		t.Fatal("server dispatched malformed request")
	}
}
