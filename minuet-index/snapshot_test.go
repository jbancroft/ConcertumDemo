package minuetindex

import (
	"context"
	"errors"
	"testing"
	"time"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// Interleave another writer after a superblock read, before any page reads.
// This makes the concurrent-split case deterministic without scheduling sleeps.
type snapshotInterleavingClient struct {
	sinfoniaclient.TxnClient
	afterSuperblock func()
}

func (c *snapshotInterleavingClient) Read(ctx context.Context, host uint32, addr uint64, length uint32) ([]byte, error) {
	data, err := c.TxnClient.Read(ctx, host, addr, length)
	if err == nil && addr == 0 && length == superblockSize && c.afterSuperblock != nil {
		c.afterSuperblock()
	}
	return data, err
}

func TestOperationsRetryConcurrentSplit(t *testing.T) {
	for _, split := range []struct {
		name string
		keys uint64
	}{{"leaf_root", 8}, {"internal_root", 40}} {
		for _, operation := range []string{"search", "scan", "insert", "delete"} {
			t.Run(split.name+"/"+operation, func(t *testing.T) {
				ctx := t.Context()
				base := newMemTxnClient()
				writer := newTestBTree(t, base, Config{HostId: 1})
				for key := uint64(1); key <= split.keys; key++ {
					if err := writer.Insert(ctx, key, key*10); err != nil {
						t.Fatal(err)
					}
				}
				splitKey := split.keys + 1
				client := &snapshotInterleavingClient{TxnClient: base}
				client.afterSuperblock = func() {
					client.afterSuperblock = nil
					// Split the full root after the reader has captured its metadata.
					if err := writer.Insert(ctx, splitKey, splitKey*10); err != nil {
						t.Fatal(err)
					}
				}
				reader := newTestBTree(t, client, Config{HostId: 1, RetryBackoff: time.Microsecond})
				switch operation {
				case "search":
					value, found, err := reader.Search(ctx, splitKey)
					if err != nil || !found || value != splitKey*10 {
						t.Fatalf("search across split: value=%d found=%t err=%v", value, found, err)
					}
				case "scan":
					it, err := reader.RangeScan(ctx, 1, splitKey)
					if err != nil {
						t.Fatal(err)
					}
					for key := uint64(1); key <= splitKey; key++ {
						entry, ok := it.Next()
						if !ok || entry != (Entry{Key: key, Value: key * 10}) {
							t.Fatalf("scan entry %d: %+v present=%t", key, entry, ok)
						}
					}
					if _, ok := it.Next(); ok {
						t.Fatal("unexpected scan entry")
					}
				case "insert":
					if err := reader.Insert(ctx, splitKey+1, 100); err != nil {
						t.Fatal(err)
					}
					value, found, err := writer.Search(ctx, splitKey+1)
					if err != nil || !found || value != 100 {
						t.Fatalf("insert across split: value=%d found=%t err=%v", value, found, err)
					}
				case "delete":
					if err := reader.Delete(ctx, 1); err != nil {
						t.Fatal(err)
					}
					if _, found, err := writer.Search(ctx, 1); err != nil || found {
						t.Fatalf("delete across split: found=%t err=%v", found, err)
					}
				}
				assertTreeInvariants(t, writer, ctx)
			})
		}
	}
}

func TestSnapshotRetriesAreBoundedAndCancelable(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		name := "retry_limit"
		if cancelRead {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			base := newMemTxnClient()
			writer := newTestBTree(t, base, Config{HostId: 1})
			if err := writer.Insert(ctx, 1, 10); err != nil {
				t.Fatal(err)
			}
			changes := 0
			client := &snapshotInterleavingClient{TxnClient: base}
			client.afterSuperblock = func() {
				changes++
				if err := writer.Insert(ctx, 1, uint64(changes)); err != nil {
					t.Fatal(err)
				}
				if cancelRead {
					cancel()
					client.afterSuperblock = nil
				}
			}
			const attempts = 3
			reader := newTestBTree(t, client, Config{HostId: 1, MaxRetries: attempts, RetryBackoff: time.Microsecond})
			_, _, err := reader.Search(ctx, 1)
			want := ErrBusy
			if cancelRead {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("search during repeated changes: %v, want %v", err, want)
			}
			if changes > 2*attempts {
				t.Fatalf("retry budget exceeded: %d changes", changes)
			}
		})
	}
}
