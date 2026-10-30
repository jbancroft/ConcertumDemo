// Command sinfonia-client-demo demonstrates the public Sinfonia client API.
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
	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const (
	demoTimeout    = time.Minute
	balanceAddress = 0 // Each node holds one balance at byte offset zero.
	balanceSize    = 8 // Balances are big-endian uint64 values.
)

func main() {
	executable := flag.String("memnode-bin", "bin/memnode", "path to the prebuilt memnode executable")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), demoTimeout)
	err := run(ctx, *executable, os.Stdout)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, executable string, out io.Writer) (err error) {
	cluster, err := democluster.Start(ctx, executable, "", balanceSize)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cluster.Close()) }()
	fmt.Fprintln(out, "Two in-memory memnode processes listening on loopback TCP")
	return demonstrate(ctx, cluster.Client, out)
}

func demonstrate(ctx context.Context, api *sinfoniaclient.Client, out io.Writer) error {
	// Write-only minitransactions initialize independent balances on nodes 1 and 2.
	for id := uint32(1); id <= 2; id++ {
		if err := api.Write(ctx, id, balanceAddress, number(100)); err != nil {
			return fmt.Errorf("initialize node %d: %w", id, err)
		}
	}
	if err := showBalances(ctx, api, out, "Initial balances", [2]uint64{100, 100}); err != nil {
		return err
	}

	// Both comparisons must succeed for either write to commit.
	result := api.NewMiniTransaction().
		Compare(1, balanceAddress, number(100)).
		Compare(2, balanceAddress, number(100)).
		Write(1, balanceAddress, number(75)).
		Write(2, balanceAddress, number(125)).
		ExecuteAndCommit(ctx)
	if err := result.OutcomeError(); err != nil {
		return err
	}
	if !result.Committed || result.Status != sinfoniaclient.StatusCommitted {
		return fmt.Errorf("transfer: committed=%t status=%d", result.Committed, result.Status)
	}
	// Commit delivery runs in the background. Wait before reading the new values.
	if err := api.FlushPendingCommits(ctx); err != nil {
		return err
	}
	if err := showBalances(ctx, api, out, "After transferring 25", [2]uint64{75, 125}); err != nil {
		return err
	}

	// Node 1 now holds 75, so this stale comparison must reject BOTH writes.
	result = api.NewMiniTransaction().
		Compare(1, balanceAddress, number(100)).
		Write(1, balanceAddress, number(0)).
		Write(2, balanceAddress, number(0)).
		ExecuteAndCommit(ctx)
	if result.Committed || result.Status != sinfoniaclient.StatusAborted {
		return fmt.Errorf("stale comparison: committed=%t status=%d, want aborted", result.Committed, result.Status)
	}
	if err := api.FlushPendingCommits(ctx); err != nil {
		return err
	}
	return showBalances(ctx, api, out, "Stale comparison aborted; balances unchanged", [2]uint64{75, 125})
}

func number(value uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, value)
}

func showBalances(ctx context.Context, api *sinfoniaclient.Client, out io.Writer, label string, want [2]uint64) error {
	var balances [2]uint64
	for i := range balances {
		id := uint32(i + 1)
		data, err := api.Read(ctx, id, balanceAddress, balanceSize)
		if err != nil {
			return fmt.Errorf("read node %d: %w", id, err)
		}
		if len(data) != balanceSize {
			return fmt.Errorf("node %d: got %d bytes, want %d", id, len(data), balanceSize)
		}
		balances[i] = binary.BigEndian.Uint64(data)
	}
	if balances != want {
		return fmt.Errorf("%s: got %v, want %v", label, balances, want)
	}
	_, err := fmt.Fprintf(out, "%s: %d, %d\n", label, balances[0], balances[1])
	return err
}
