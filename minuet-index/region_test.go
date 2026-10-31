package minuetindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

// Fail at the client boundary if any tree operation escapes its region.
type boundedTestClient struct {
	sinfoniaclient.TxnClient
	t       *testing.T
	base    uint64
	size    uint64
	commits int
}

func (c *boundedTestClient) check(addr uint64, length int) {
	c.t.Helper()
	if addr < c.base || addr-c.base > c.size || uint64(length) > c.size-(addr-c.base) {
		c.t.Fatalf("access [%d, +%d) escapes region [%d, +%d)", addr, length, c.base, c.size)
	}
}

func (c *boundedTestClient) Read(ctx context.Context, host uint32, addr uint64, length uint32) ([]byte, error) {
	c.check(addr, int(length))
	return c.TxnClient.Read(ctx, host, addr, length)
}

func (c *boundedTestClient) ExecuteAndCommit(ctx context.Context, compares []sinfoniaclient.CompareItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	for _, item := range compares {
		c.check(item.Address, len(item.Data))
	}
	for _, item := range reads {
		c.check(item.Address, int(item.Length))
	}
	for _, item := range writes {
		c.check(item.Address, len(item.Data))
	}
	c.commits++
	return c.TxnClient.ExecuteAndCommit(ctx, compares, reads, writes)
}

func TestNewBTreeRejectsInvalidRegions(t *testing.T) {
	for _, cfg := range []Config{
		{}, {RegionSize: MinRegionSize - 1},
		{BaseAddress: math.MaxUint64 - MinRegionSize + 1, RegionSize: MinRegionSize},
		{RegionSize: superblockSize + uint64(math.MaxUint32)*pageSize},
	} {
		if _, err := NewBTree(newMemTxnClient(), cfg); !errors.Is(err, ErrInvalidRegion) {
			t.Errorf("config %+v: got %v, want ErrInvalidRegion", cfg, err)
		}
	}
}

func TestRegionFullDiscardsEntireSplit(t *testing.T) {
	for _, tc := range []struct {
		pages, tail, keys int
	}{
		{pages: 1, keys: 8},
		{pages: 2, keys: 8}, // A leaf split fits, but its new root does not.
		{pages: 3, keys: 12},
		{pages: 12, keys: 40}, // A leaf and internal split fit, but the new root does not.
		{pages: 12, tail: pageSize - 1, keys: 40},
	} {
		t.Run(fmt.Sprintf("pages_%d_tail_%d", tc.pages, tc.tail), func(t *testing.T) {
			ctx := context.Background()
			base := newMemTxnClient()
			cfg := Config{HostId: 1, BaseAddress: 1024, RegionSize: uint64(superblockSize + tc.pages*pageSize + tc.tail)}
			client := &boundedTestClient{TxnClient: base, t: t, base: cfg.BaseAddress, size: cfg.RegionSize}
			idx := newTestBTree(t, client, cfg)
			for key := 1; key <= tc.keys; key++ {
				if err := idx.Insert(ctx, uint64(key), uint64(key*10)); err != nil {
					t.Fatalf("insert %d: %v", key, err)
				}
			}
			before := bytes.Clone(base.ram)
			commits := client.commits
			if err := idx.Insert(ctx, uint64(tc.keys+1), 999); !errors.Is(err, ErrRegionFull) {
				t.Fatalf("overflow insert: %v", err)
			}
			if client.commits != commits || !bytes.Equal(before, base.ram) {
				t.Fatal("failed split attempted a transaction or changed stored bytes")
			}
			idx = newTestBTree(t, client, cfg)
			assertTreeInvariants(t, idx, ctx)
			it, err := idx.RangeScan(ctx, 0, math.MaxUint64)
			if err != nil {
				t.Fatal(err)
			}
			for key := 1; key <= tc.keys; key++ {
				entry, ok := it.Next()
				if !ok || entry != (Entry{Key: uint64(key), Value: uint64(key * 10)}) {
					t.Fatalf("entry %d: %+v, present=%t", key, entry, ok)
				}
			}
			if _, ok := it.Next(); ok {
				t.Fatal("failed insert appeared in the tree")
			}
			// Exhaustion only prevents allocations; an overwrite and a delete
			// followed by an insert that fits an existing leaf still work.
			if err := idx.Insert(ctx, 1, 777); err != nil {
				t.Fatal(err)
			}
			if value, found, err := idx.Search(ctx, 1); err != nil || !found || value != 777 {
				t.Fatalf("overwrite: %d %t %v", value, found, err)
			}
			if err := idx.Delete(ctx, 1); err != nil {
				t.Fatal(err)
			}
			if err := idx.Insert(ctx, 1, 10); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTreeRejectsChangedRegionOnReopen(t *testing.T) {
	ctx := context.Background()
	client := newMemTxnClient()
	cfg := Config{HostId: 1, RegionSize: MinRegionSize}
	idx := newTestBTree(t, client, cfg)
	if err := idx.Insert(ctx, 1, 10); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(client.ram)
	cfg.RegionSize += pageSize
	reopened := newTestBTree(t, client, cfg)
	if err := reopened.Insert(ctx, 2, 20); !errors.Is(err, ErrInvalidRegion) {
		t.Fatalf("resized tree error: %v", err)
	}
	if !bytes.Equal(before, client.ram) {
		t.Fatal("rejected resize changed storage")
	}
}

func TestTreeRejectsCorruptBoundsBeforeAccess(t *testing.T) {
	for _, field := range []string{"magic", "next_page", "root", "child", "leaf_link"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			base := newMemTxnClient()
			cfg := Config{HostId: 1, RegionSize: superblockSize + 3*pageSize}
			client := &boundedTestClient{TxnClient: base, t: t, size: cfg.RegionSize}
			idx := newTestBTree(t, client, cfg)
			for key := uint64(1); key <= 9; key++ {
				if err := idx.Insert(ctx, key, key); err != nil {
					t.Fatal(err)
				}
			}
			switch field {
			case "magic":
				base.ram[0] ^= 0xff
			case "next_page":
				binary.BigEndian.PutUint32(base.ram[superNextPageOffset:], math.MaxUint32)
			case "root":
				binary.BigEndian.PutUint32(base.ram[superRootPageOffset:], math.MaxUint32)
			case "child":
				root := binary.BigEndian.Uint32(base.ram[superRootPageOffset:])
				addr, err := idx.pageAddr(root)
				if err != nil {
					t.Fatal(err)
				}
				// This root has one separator followed by its child IDs.
				binary.BigEndian.PutUint32(base.ram[addr+nodeHeaderSize+keySize:], math.MaxUint32)
			case "leaf_link":
				binary.BigEndian.PutUint32(base.ram[superblockSize+nodeNextLeafOffset:], math.MaxUint32)
			}
			before := bytes.Clone(base.ram)
			commits := client.commits
			if err := idx.Insert(ctx, 10, 10); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt %s: %v", field, err)
			}
			if !bytes.Equal(before, base.ram) || client.commits != commits {
				t.Fatal("damaged tree attempted a transaction or changed storage")
			}
		})
	}
}
