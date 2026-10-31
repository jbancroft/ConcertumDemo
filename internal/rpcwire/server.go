package rpcwire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// Handler must observe context cancellation when waiting for work or resources.
type Handler func(context.Context, any) (any, error)

type Server struct {
	connContext func(net.Conn) context.Context

	mu      sync.RWMutex
	methods map[string]Handler
}

func NewServer(connContext func(net.Conn) context.Context) *Server {
	return &Server{
		connContext: connContext,
		methods:     map[string]Handler{},
	}
}

func (s *Server) Register(method string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods[method] = handler
}

func RegisterTyped[Req any, Resp any](s *Server, method string, fn func(context.Context, Req) (Resp, error)) {
	s.Register(method, func(ctx context.Context, body any) (any, error) {
		req, ok := body.(Req)
		if !ok {
			var zero Resp
			return zero, fmt.Errorf("unexpected request type %T for %s", body, method)
		}
		return fn(ctx, req)
	})
}

func (s *Server) Serve(listener net.Listener) error {
	// Closing the listener cancels blocked handlers and closes idle connections.
	// Drain all handlers before the caller closes participant storage.
	var mu sync.Mutex
	var handlers sync.WaitGroup
	connections := make(map[net.Conn]context.CancelFunc)
	defer func() {
		mu.Lock()
		for conn, cancel := range connections {
			cancel()
			_ = conn.Close()
		}
		mu.Unlock()
		handlers.Wait()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		ctx := context.Background()
		if s.connContext != nil {
			if connCtx := s.connContext(conn); connCtx != nil {
				ctx = connCtx
			}
		}
		ctx, cancel := context.WithCancel(ctx)
		mu.Lock()
		connections[conn] = cancel
		mu.Unlock()
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer cancel()
			defer func() {
				mu.Lock()
				delete(connections, conn)
				mu.Unlock()
			}()
			s.serveConn(ctx, conn)
		}()
	}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for {
		var req Request
		if err := readFrame(conn, &req); err != nil {
			return
		}
		resp := Response{}
		handler, ok := s.lookup(req.Method)
		if !ok {
			resp.Error = fmt.Sprintf("unknown rpc method %q", req.Method)
		} else {
			out, err := handler(ctx, req.Body)
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.Body = out
			}
		}
		if err := writeFrame(conn, resp); err != nil {
			return
		}
	}
}

func (s *Server) lookup(method string) (Handler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.methods[method]
	return h, ok
}
