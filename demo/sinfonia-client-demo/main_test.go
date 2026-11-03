package main

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/democluster"
)

func TestDemonstrationPreservesBalancesAfterAbortedTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), demoTimeout)
	defer cancel()
	executable := os.Getenv("MEMNODE_BIN")
	cluster, err := democluster.Start(ctx, executable, t.TempDir(), balanceSize)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(); err != nil {
			t.Error(err)
		}
	}()
	api := cluster.Client
	if err := demonstrate(ctx, api, io.Discard); err != nil {
		t.Fatal(err)
	}
	// Inspect actual stored bytes, independently of the demo's printed messages.
	for i, want := range []uint64{75, 125} {
		id := uint32(i + 1)
		data, err := api.Read(ctx, id, 0, 8)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 8 || binary.BigEndian.Uint64(data) != want {
			t.Errorf("node %d: got %x, want balance %d", id, data, want)
		}
	}
}
