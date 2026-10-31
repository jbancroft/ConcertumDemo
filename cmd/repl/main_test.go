package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	minuetsql "github.com/jbancroft/ConcertumDemo/minuet-sql"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

func TestREPLCommandsAndUnknownOutcome(t *testing.T) {
	input := "\n.help\n.unknown\nCREATE TABLE scores;  \nSELECT id, value FROM scores;\n.quit\nDELETE FROM scores WHERE id = 1;\n"
	var output bytes.Buffer
	var statements []string
	execute := func(_ context.Context, sql string) (minuetsql.QueryResult, error) {
		statements = append(statements, sql)
		if strings.HasPrefix(sql, "CREATE") {
			return minuetsql.QueryResult{}, fmt.Errorf("lost reply: %w", sinfoniaclient.ErrUnknownOutcome)
		}
		return minuetsql.QueryResult{}, nil
	}
	if err := repl(context.Background(), execute, io.NopCloser(strings.NewReader(input)), &output); err != nil {
		t.Fatal(err)
	}
	want := []string{"CREATE TABLE scores;", "SELECT id, value FROM scores;"}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("executed %q, want %q (no automatic retry or execution after .quit)", statements, want)
	}
	for _, text := range []string{"Enter one SQL statement per line", "Unknown command", "may have committed", "(0 rows)"} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("missing %q in output:\n%s", text, output.String())
		}
	}
	if strings.Contains(output.String(), "OK") {
		t.Fatal("uncertain creation was reported as successful")
	}
}

func TestREPLOverTCPAndDurableReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dir := t.TempDir()
	args, stop := startREPLNode(t, ctx, dir)
	var output bytes.Buffer
	input := strings.Join([]string{
		"CREATE TABLE scores;",
		"INSERT INTO scores VALUES (2, 250);",
		"INSERT INTO scores VALUES (1, 100);",
		"INSERT INTO scores VALUES (1, 999);", // Errors must leave the session usable.
		"UPDATE scores SET value = 0;",
		"SELECT id, value FROM scores;",
		"DELETE FROM scores WHERE id = 1;",
		"SELECT id, value FROM scores WHERE id = 1;",
		".quit",
		"DELETE FROM scores WHERE id = 2;", // Quitting must not execute pending input.
	}, "\n")
	if err := run(ctx, args, io.NopCloser(strings.NewReader(input)), &output, &output); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"duplicate key", "unsupported SQL", "id  value\n1   100\n2   250\n(2 rows)", "(0 rows)"} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("missing %q in output:\n%s", text, output.String())
		}
	}
	output.Reset()
	// Quitting the REPL must leave the external node and its data available.
	// EOF without a final newline also exits normally.
	if err := run(ctx, args, io.NopCloser(strings.NewReader("SELECT id, value FROM scores;")), &output, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "id  value\n2   250\n(1 row)") {
		t.Fatalf("rows were not available to a second session:\n%s", output.String())
	}
	stop()
	args, _ = startREPLNode(t, ctx, dir)
	output.Reset()
	if err := run(ctx, args, io.NopCloser(strings.NewReader("SELECT id, value FROM scores;\n.quit\n")), &output, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "id  value\n2   250\n(1 row)") {
		t.Fatalf("saved rows were not reopened:\n%s", output.String())
	}
}

// The test owns the node independently of the REPL, just like a separate server.
func startREPLNode(t *testing.T, ctx context.Context, dir string) ([]string, func()) {
	t.Helper()
	node, err := memnode.NewDurableParticipant(1, nodeMemorySize, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- node.RPCServer().Serve(listener) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return []string{"-host", host, "-port", port}, stop
}

func TestREPLRejectsInvalidAddress(t *testing.T) {
	for _, args := range [][]string{
		{"-host", ""}, {"-port", "0"}, {"-port", "-1"},
		{"-port", "65536"}, {"unexpected-argument"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if err := run(context.Background(), args, io.NopCloser(strings.NewReader(".quit\n")), io.Discard, io.Discard); err == nil {
				t.Fatal("invalid address was accepted")
			}
		})
	}
}

// Signal when the prompt is visible, without reading a buffer concurrently.
type promptWriter struct {
	ready chan struct{}
}

func (w *promptWriter) Write(p []byte) (int, error) {
	if string(p) == "sql> " && w.ready != nil {
		close(w.ready)
		w.ready = nil
	}
	return len(p), nil
}

func TestREPLCancellationWhileInputIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input, writer := io.Pipe()
	defer writer.Close()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- repl(ctx, func(context.Context, string) (minuetsql.QueryResult, error) {
			return minuetsql.QueryResult{}, errors.New("unexpected SQL execution")
		}, input, &promptWriter{ready: ready})
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not appear")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not stop the blocked console")
	}
}

func TestREPLCancellationPreservesUnknownOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	execute := func(context.Context, string) (minuetsql.QueryResult, error) {
		cancel()
		return minuetsql.QueryResult{}, sinfoniaclient.ErrUnknownOutcome
	}
	if err := repl(ctx, execute, io.NopCloser(strings.NewReader("CREATE TABLE scores\n")), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "may have committed") {
		t.Fatalf("cancellation hid the uncertain result:\n%s", output.String())
	}
}

func TestREPLRejectsOversizedInput(t *testing.T) {
	calls := 0
	execute := func(context.Context, string) (minuetsql.QueryResult, error) {
		calls++
		return minuetsql.QueryResult{}, nil
	}
	input := strings.NewReader(strings.Repeat("x", maxLineBytes) + "\nCREATE TABLE scores\n")
	err := repl(context.Background(), execute, io.NopCloser(input), io.Discard)
	if err == nil || calls != 0 {
		t.Fatalf("oversized input: error=%v, executed=%d", err, calls)
	}
}
