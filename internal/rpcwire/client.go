package rpcwire

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
)

const (
	defaultCallTimeout = 5 * time.Second
	defaultDialTimout  = 5 * time.Second
	defaultMaxIdle     = 4
	maxFrameSize       = 64 << 20
)

type Request struct {
	Method string
	Body   any
}

type Response struct {
	Body  any
	Error string
}

type RemoteError struct {
	Method  string
	Message string
}

func (e *RemoteError) Error() string {
	if e == nil {
		return ""
	}
	if e.Method == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Method, e.Message)
}

type ClientOptions struct {
	TLS         *tls.Config
	DialTimeout time.Duration
	CallTimeout time.Duration
	MaxIdle     int
}

type Client struct {
	addr string
	opts ClientOptions

	mu   sync.Mutex
	idle []net.Conn
}

func NewClient(addr string, opts ClientOptions) *Client {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaultDialTimout
	}
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = defaultCallTimeout
	}
	if opts.MaxIdle <= 0 {
		opts.MaxIdle = defaultMaxIdle
	}
	return &Client{addr: addr, opts: opts}
}

func (c *Client) Call(ctx context.Context, method string, req any, resp any) error {
	timeout := c.opts.CallTimeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := c.borrow(ctx)
	if err != nil {
		return err
	}
	reusable := true
	defer func() {
		c.release(conn, reusable)
	}()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			reusable = false
			return err
		}
	} else if err := conn.SetDeadline(time.Time{}); err != nil {
		reusable = false
		return err
	}

	// Closing the connection interrupts blocked I/O even for cancellation before
	// the deadline. Join the callback before returning a connection to the pool.
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(cancelled) })
	defer func() {
		if !stop() {
			<-cancelled
			reusable = false
		}
	}()
	if err := writeFrame(conn, Request{Method: method, Body: req}); err != nil {
		reusable = false
		return err
	}
	var out Response
	if err := readFrame(conn, &out); err != nil {
		reusable = false
		return err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		reusable = false
		return err
	}
	if out.Error != "" {
		return &RemoteError{Method: method, Message: out.Error}
	}
	if err := assignBody(resp, out.Body); err != nil {
		reusable = false
		return err
	}
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, conn := range c.idle {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.idle = nil
	return firstErr
}

func (c *Client) borrow(ctx context.Context) (net.Conn, error) {
	c.mu.Lock()
	n := len(c.idle)
	if n > 0 {
		conn := c.idle[n-1]
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()
	return c.dial(ctx)
}

func (c *Client) release(conn net.Conn, reusable bool) {
	if conn == nil {
		return
	}
	if !reusable {
		_ = conn.Close()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.idle) >= c.opts.MaxIdle {
		_ = conn.Close()
		return
	}
	c.idle = append(c.idle, conn)
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: c.opts.DialTimeout}
	if c.opts.TLS != nil {
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: c.opts.TLS}
		return tlsDialer.DialContext(ctx, "tcp", c.addr)
	}
	return dialer.DialContext(ctx, "tcp", c.addr)
}

func assignBody(dst any, body any) error {
	if dst == nil || body == nil {
		return nil
	}
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("response target must be a non-nil pointer")
	}
	value := reflect.ValueOf(body)
	if !value.Type().AssignableTo(rv.Elem().Type()) {
		if value.Type().ConvertibleTo(rv.Elem().Type()) {
			value = value.Convert(rv.Elem().Type())
		} else {
			return fmt.Errorf("unexpected response type %T for %T", body, dst)
		}
	}
	rv.Elem().Set(value)
	return nil
}

func writeFrame(w io.Writer, v any) error {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(v); err != nil {
		return err
	}
	if payload.Len() > maxFrameSize {
		return fmt.Errorf("rpc frame too large: %d", payload.Len())
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(payload.Len()))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload.Bytes())
}

func readFrame(r io.Reader, out any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 {
		return io.EOF
	}
	if n > maxFrameSize {
		return fmt.Errorf("rpc frame too large: %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(payload)).Decode(out)
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(buf) {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}
