// Command repl runs a small SQL console connected to an existing memnode.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	minuetsql "github.com/jbancroft/ConcertumDemo/minuet-sql"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const (
	nodeMemorySize = 2 << 20 // Use the default memnode's 2 MiB region.
	sqlHostID      = 1
	maxTCPPort     = 1<<16 - 1
	flushTimeout   = 10 * time.Second
	maxLineBytes   = 64 << 10
)

const helpText = `Enter one SQL statement per line; a trailing semicolon is optional.
Tables have two unsigned integer columns: id and value.

  CREATE TABLE scores;
  INSERT INTO scores VALUES (1, 100);
  SELECT id, value FROM scores;
  SELECT id, value FROM scores WHERE id = 1;
  DELETE FROM scores WHERE id = 1;

.help  Show these examples
.quit  Exit (EOF or Ctrl-C also exits)

Up to six tables, each with a fixed 64 KiB region. No joins or SQL transactions.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.ReadCloser, out, errOut io.Writer) (err error) {
	flags := flag.NewFlagSet("repl", flag.ContinueOnError)
	flags.SetOutput(errOut)
	host := flags.String("host", "127.0.0.1", "memnode host (node ID 1, at least 2 MiB)")
	port := flags.Int("port", 7001, "memnode RPC port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("expected flags only; enter SQL at the prompt")
	}
	if *host == "" || *port < 1 || *port > maxTCPPort {
		return fmt.Errorf("expected a nonempty host and a port between 1 and %d", maxTCPPort)
	}
	address := net.JoinHostPort(*host, strconv.Itoa(*port))
	client := sinfoniaclient.NewClient(map[uint32]sinfoniaclient.NodeConfig{
		sqlHostID: {RPCAddress: address},
	})
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		defer cancel()
		err = errors.Join(err, client.FlushPendingCommits(flushCtx))
	}()
	engine, err := minuetsql.NewEngine(minuetsql.Config{
		HostId: sqlHostID, TxnClient: client, MemorySize: nodeMemorySize,
	})
	if err != nil {
		return fmt.Errorf("open SQL on memnode %s: %w", address, err)
	}
	fmt.Fprintln(out, "Concertum SQL REPL")
	fmt.Fprintf(out, "Connected to memnode %d at %s.\n", sqlHostID, address)
	fmt.Fprintln(out, "Type .help for examples, .quit to exit.")
	return repl(ctx, engine.Exec, input, out)
}

type inputLine struct {
	text string
	err  error
}

func repl(ctx context.Context, execute func(context.Context, string) (minuetsql.QueryResult, error), input io.ReadCloser, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer input.Close()
	// Read separately so Ctrl-C can stop the session while stdin is blocked.
	lines := make(chan inputLine)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(nil, maxLineBytes)
		for scanner.Scan() {
			select {
			case lines <- inputLine{text: scanner.Text()}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case lines <- inputLine{err: err}:
			case <-ctx.Done():
			}
		}
	}()
	for {
		if ctx.Err() != nil {
			fmt.Fprintln(out)
			return nil
		}
		fmt.Fprint(out, "sql> ")
		select {
		case <-ctx.Done():
			fmt.Fprintln(out)
			return nil
		case line, ok := <-lines:
			if !ok {
				fmt.Fprintln(out)
				return nil
			}
			if line.err != nil {
				return fmt.Errorf("read SQL input (maximum line size %d bytes): %w", maxLineBytes, line.err)
			}
			sql := strings.TrimSpace(line.text)
			switch sql {
			case "":
				continue
			case ".help":
				fmt.Fprint(out, helpText)
				continue
			case ".quit":
				return nil
			}
			if strings.HasPrefix(sql, ".") {
				fmt.Fprintln(out, "Unknown command. Use .help or .quit.")
				continue
			}
			result, err := execute(ctx, sql)
			if errors.Is(err, sinfoniaclient.ErrUnknownOutcome) {
				fmt.Fprintln(out, "Outcome unknown: the statement may have committed. Check stored state before retrying.")
			} else if err != nil {
				fmt.Fprintf(out, "Error: %v\n", err)
			} else if strings.HasPrefix(strings.ToUpper(sql), "SELECT ") {
				if err := printRows(out, result.Rows); err != nil {
					return err
				}
			} else {
				fmt.Fprintln(out, "OK")
			}
		}
	}
}

func printRows(out io.Writer, rows []minuetsql.Row) error {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "id\tvalue")
	for _, row := range rows {
		fmt.Fprintf(table, "%d\t%d\n", row.ID, row.Value)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	label := "rows"
	if len(rows) == 1 {
		label = "row"
	}
	_, err := fmt.Fprintf(out, "(%d %s)\n", len(rows), label)
	return err
}
