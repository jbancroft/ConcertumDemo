// Command demo runs transactions, an index, and SQL on two local nodes.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/democluster"
	minuetindex "github.com/jbancroft/ConcertumDemo/minuet-index"
	minuetsql "github.com/jbancroft/ConcertumDemo/minuet-sql"
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const balanceAddr = 768 // Separate from the SQL catalog and index pages.

const (
	demoTimeout     = time.Minute
	nodeMemorySize  = 2 << 20  // 2 MiB
	indexBaseAddr   = 64 << 10 // 64 KiB
	indexRegionSize = 64 << 10
)

func main() {
	executable := flag.String("memnode-bin", "bin/memnode", "path to the prebuilt memnode executable")
	dataRoot := flag.String("data-dir", "tmp/demo", "parent directory for a fresh demo run")
	flag.Parse()
	if err := os.MkdirAll(*dataRoot, 0o755); err != nil {
		fail(err)
	}
	dir, err := os.MkdirTemp(*dataRoot, "run-")
	if err != nil {
		fail(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), demoTimeout)
	defer cancel()
	if err := run(ctx, *executable, dir, os.Stdout); err != nil {
		fail(err)
	}
	fmt.Printf("Demo data retained in %s\n", dir)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func run(ctx context.Context, executable, dir string, out io.Writer) (err error) {
	c, err := democluster.Start(ctx, executable, dir, nodeMemorySize)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.Close()) }()
	fmt.Fprintln(out, "Two durable memnode processes listening on loopback TCP")

	if err := commit(ctx, c.Client, c.Client.NewMiniTransaction().
		Write(1, balanceAddr, number(100)).Write(2, balanceAddr, number(100))); err != nil {
		return err
	}
	if err := commit(ctx, c.Client, c.Client.NewMiniTransaction().
		Compare(1, balanceAddr, number(100)).Compare(2, balanceAddr, number(100)).
		Write(1, balanceAddr, number(75)).Write(2, balanceAddr, number(125))); err != nil {
		return err
	}
	// The balance is now 75, so comparing it with 100 must reject both writes.
	result := c.Client.NewMiniTransaction().Compare(1, balanceAddr, number(100)).
		Write(1, balanceAddr, number(0)).Write(2, balanceAddr, number(0)).ExecuteAndCommit(ctx)
	if result.Committed || result.Status != sinfoniaclient.StatusAborted {
		return fmt.Errorf("stale compare: committed=%t status=%d", result.Committed, result.Status)
	}
	if err := c.Client.FlushPendingCommits(ctx); err != nil {
		return err
	}
	if err := checkBalances(ctx, c.Client); err != nil {
		return err
	}
	fmt.Fprintln(out, "Sinfonia: transfer committed; stale compare aborted; balances = 75, 125")

	index, err := newIndex(c.Client)
	if err != nil {
		return err
	}
	for key := uint64(1); key <= 40; key++ {
		if err := index.Insert(ctx, key, key*10); err != nil {
			return err
		}
	}
	for key := uint64(1); key <= 32; key++ {
		if err := index.Delete(ctx, key); err != nil {
			return err
		}
	}
	if err := checkIndex(ctx, index); err != nil {
		return err
	}
	fmt.Fprintln(out, "Minuet: inserted 40 keys; deleted 32; ordered range = 33..40")

	sql, err := newSQL(c.Client)
	if err != nil {
		return err
	}
	for _, statement := range []string{
		"CREATE TABLE scores", "INSERT INTO scores VALUES (1, 100)",
		"INSERT INTO scores VALUES (2, 250)",
	} {
		if _, err := sql.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if err := checkSQL(ctx, sql); err != nil {
		return err
	}
	fmt.Fprintln(out, "SQL: SELECT id, value FROM scores -> [{1 100} {2 250}]")

	if err := c.Close(); err != nil {
		return err
	}
	c, err = democluster.Start(ctx, executable, dir, nodeMemorySize)
	if err != nil {
		return err
	}
	if err := checkBalances(ctx, c.Client); err != nil {
		return err
	}
	index, err = newIndex(c.Client)
	if err != nil {
		return err
	}
	if err := checkIndex(ctx, index); err != nil {
		return err
	}
	sql, err = newSQL(c.Client)
	if err != nil {
		return err
	}
	if err := checkSQL(ctx, sql); err != nil {
		return err
	}
	fmt.Fprintln(out, "Recovery: reopened both nodes; balances, index, and SQL catalog/data verified")
	return nil
}

func number(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

func commit(ctx context.Context, api *sinfoniaclient.Client, tx *sinfoniaclient.MiniTransaction) error {
	result := tx.ExecuteAndCommit(ctx)
	if err := result.OutcomeError(); err != nil {
		return err
	}
	if !result.Committed {
		return fmt.Errorf("transaction did not commit: status=%d", result.Status)
	}
	// Wait for the background commit messages before checking the saved values.
	return api.FlushPendingCommits(ctx)
}

func checkBalances(ctx context.Context, api *sinfoniaclient.Client) error {
	for i, want := range []uint64{75, 125} {
		data, err := api.Read(ctx, uint32(i+1), balanceAddr, 8)
		if err != nil {
			return err
		}
		if len(data) != 8 || binary.BigEndian.Uint64(data) != want {
			return fmt.Errorf("balance on participant %d: got %x, want %d", i+1, data, want)
		}
	}
	return nil
}

func newIndex(api *sinfoniaclient.Client) (*minuetindex.BTree, error) {
	return minuetindex.NewBTree(api,
		minuetindex.Config{HostId: 1, BaseAddress: indexBaseAddr, RegionSize: indexRegionSize})
}

func checkIndex(ctx context.Context, index *minuetindex.BTree) error {
	it, err := index.RangeScan(ctx, 0, 100)
	if err != nil {
		return err
	}
	for key := uint64(33); key <= 40; key++ {
		entry, ok := it.Next()
		if !ok || entry.Key != key || entry.Value != key*10 {
			return fmt.Errorf("range entry: got %+v present=%t, want key %d", entry, ok, key)
		}
	}
	if _, ok := it.Next(); ok {
		return errors.New("unexpected extra range entry")
	}
	value, found, err := index.Search(ctx, 40)
	if err != nil || !found || value != 400 {
		return fmt.Errorf("search: value=%d found=%t err=%v", value, found, err)
	}
	return nil
}

func newSQL(api *sinfoniaclient.Client) (*minuetsql.Engine, error) {
	return minuetsql.NewEngine(minuetsql.Config{HostId: 2, TxnClient: api, MemorySize: nodeMemorySize})
}

func checkSQL(ctx context.Context, engine *minuetsql.Engine) error {
	result, err := engine.Exec(ctx, "SELECT id, value FROM scores")
	if err != nil {
		return err
	}
	if len(result.Rows) != 2 || result.Rows[0] != (minuetsql.Row{ID: 1, Value: 100}) ||
		result.Rows[1] != (minuetsql.Row{ID: 2, Value: 250}) {
		return fmt.Errorf("unexpected SQL rows: %+v", result.Rows)
	}
	return nil
}
