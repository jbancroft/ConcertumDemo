// Command repl runs a small SQL console on local memnode processes.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/jbancroft/ConcertumDemo/internal/democluster"
	minuetsql "github.com/jbancroft/ConcertumDemo/minuet-sql"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const (
	nodeMemorySize = 2 << 20 // 2 MiB per node.
	sqlHostID      = 2       // Same SQL storage node as cmd/demo.
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
	executable := flags.String("memnode-bin", "bin/memnode", "path to the prebuilt memnode executable")
	dir := flags.String("data-dir", "", "save and reopen data in this directory; empty starts in memory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("expected flags only; enter SQL at the prompt")
	}
	cluster, err := democluster.Start(ctx, *executable, *dir, nodeMemorySize)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cluster.Close()) }()
	engine, err := minuetsql.NewEngine(minuetsql.Config{
		HostId: sqlHostID, TxnClient: cluster.Client, MemorySize: nodeMemorySize,
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "Concertum SQL REPL")
	if *dir == "" {
		fmt.Fprintln(out, "Fresh in-memory session; data is discarded on exit.")
	} else {
		fmt.Fprintf(out, "Data directory: %s\n", *dir)
	}
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
