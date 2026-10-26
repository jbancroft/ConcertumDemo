package rpcwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
)

type Handler func(context.Context, any) (any, error)

type Server struct {
	logger      *slog.Logger
	connContext func(net.Conn) context.Context

	mu      sync.RWMutex
	methods map[string]Handler
}

func NewServer(logger *slog.Logger, connContext func(net.Conn) context.Context) *Server {
	return &Server{
		logger:      logger,
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
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	ctx := context.Background()
	if s.connContext != nil {
		if connCtx := s.connContext(conn); connCtx != nil {
			ctx = connCtx
		}
	}
	for {
		var req Request
		if err := readFrame(conn, &req); err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) && s.logger != nil {
				s.logger.Debug("rpc connection closed", "err", err)
			}
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
			if s.logger != nil {
				s.logger.Debug("rpc write failed", "err", err)
			}
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
