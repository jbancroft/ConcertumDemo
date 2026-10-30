package minuetsql

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/democluster"
)

func TestTableRegionsSurviveDurableRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dir := t.TempDir()
	cluster, err := democluster.Start(ctx, os.Getenv("MEMNODE_BIN"), dir, testMemorySize)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(); err != nil {
			t.Error(err)
		}
	}()
	e := newRegionTestEngine(t, cluster.Client, smallTableRegionSize, testMemorySize)
	fillAdjacentTables(t, e)
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := democluster.Start(ctx, os.Getenv("MEMNODE_BIN"), dir, testMemorySize)
	if err != nil {
		t.Fatal(err)
	}
	cluster = reopened
	e = newRegionTestEngine(t, cluster.Client, 2*smallTableRegionSize, testMemorySize)
	checkAdjacentTables(t, e)
	if _, err := e.Exec(ctx, "INSERT INTO alpha VALUES (13, 130)"); !errors.Is(err, ErrTableFull) {
		t.Fatalf("reopened full table: %v", err)
	}
	checkAdjacentTables(t, e)
}
