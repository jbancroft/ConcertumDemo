package minuetindex

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sinfoniaclient "github.com/jbancroft/concertum2/sinfonia-client"
)

type memTxnClient struct {
	mu  sync.Mutex
	ram []byte
}

func newMemTxnClient() *memTxnClient { return &memTxnClient{ram: make([]byte, 1<<20)} }

func TestDecodeNodeRejectsTruncatedPages(t *testing.T) {
	if _, err := decodeNode(make([]byte, 23)); err == nil {
		t.Fatal("expected truncated header error")
	}
	page := make([]byte, pageSize)
	binary.BigEndian.PutUint32(page[0:4], pageMagic)
	binary.BigEndian.PutUint16(page[10:12], ^uint16(0))
	if _, err := decodeNode(page); err == nil {
		t.Fatal("expected truncated payload error")
	}
}

func FuzzDecodeNodeNeverPanics(f *testing.F) {
	f.Add(make([]byte, 0))
	f.Add(make([]byte, pageSize))
	f.Fuzz(func(t *testing.T, page []byte) {
		_, _ = decodeNode(page)
	})
}

func (m *memTxnClient) Read(_ context.Context, _ uint32, addr uint64, length uint32) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]byte, length)
	copy(out, m.ram[int(addr):int(addr)+int(length)])
	return out, nil
}

func (m *memTxnClient) Write(_ context.Context, _ uint32, addr uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy(m.ram[int(addr):int(addr)+len(data)], data)
	return nil
}

func (m *memTxnClient) ExecAndCommit(_ context.Context, _ string, cmp []sinfoniaclient.CmpItem, _ []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range cmp {
		if string(m.ram[int(c.Addr):int(c.Addr)+len(c.Data)]) != string(c.Data) {
			return sinfoniaclient.ExecResult{Committed: false, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
		}
	}
	for _, w := range writes {
		copy(m.ram[int(w.Addr):int(w.Addr)+len(w.Data)], w.Data)
	}
	return sinfoniaclient.ExecResult{Committed: true, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
}

func TestInsertSearchDeleteSplitMergeRegression(t *testing.T) {
	idx := New(newMemTxnClient(), Config{MemID: 1, MaxRetries: 20, RetryBackoff: time.Microsecond})
	ctx := context.Background()
	for i := uint64(1); i <= 80; i++ {
		if err := idx.Insert(ctx, i, i*10); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	assertTreeInvariants(t, idx, ctx)
	for i := uint64(1); i <= 80; i++ {
		v, ok, err := idx.Search(ctx, i)
		if err != nil || !ok || v != i*10 {
			t.Fatalf("search %d => v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
	for i := uint64(1); i <= 80; i += 2 {
		if err := idx.Delete(ctx, i); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	assertTreeInvariants(t, idx, ctx)
	for i := uint64(1); i <= 80; i++ {
		_, ok, err := idx.Search(ctx, i)
		if err != nil {
			t.Fatalf("search %d: %v", i, err)
		}
		if (i%2 == 1) == ok {
			t.Fatalf("unexpected presence for key %d: %v", i, ok)
		}
	}
}

func TestRangeScanIteratorStableSnapshot(t *testing.T) {
	idx := New(newMemTxnClient(), Config{MemID: 1, MaxRetries: 20, RetryBackoff: time.Microsecond})
	ctx := context.Background()
	for i := uint64(1); i <= 30; i++ {
		_ = idx.Insert(ctx, i, i)
	}
	it, err := idx.RangeScan(ctx, 10, 20)
	if err != nil {
		t.Fatalf("rangescan: %v", err)
	}
	for i := uint64(10); i <= 20; i++ {
		_ = idx.Delete(ctx, i)
	}
	got := make([]uint64, 0)
	for {
		e, ok := it.Next()
		if !ok {
			break
		}
		got = append(got, e.Key)
	}
	if len(got) != 11 || got[0] != 10 || got[len(got)-1] != 20 {
		t.Fatalf("iterator not stable snapshot: %v", got)
	}
}

func TestRangeScanReturnsEmptyForUnmatchedRanges(t *testing.T) {
	idx := New(newMemTxnClient(), Config{MemID: 1})
	if err := idx.Insert(context.Background(), 10, 100); err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][2]uint64{{0, 9}, {11, 20}} {
		it, err := idx.RangeScan(context.Background(), tc[0], tc[1])
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := it.Next(); ok {
			t.Fatalf("range %v returned an entry", tc)
		}
	}
}

func TestPropertyOrderingAndModelParity(t *testing.T) {
	rng := rand.New(rand.NewSource(77))
	idx := New(newMemTxnClient(), Config{MemID: 1, MaxRetries: 40, RetryBackoff: time.Microsecond})
	ctx := context.Background()
	model := map[uint64]uint64{}
	for step := 0; step < 800; step++ {
		k := uint64(rng.Intn(100))
		op := rng.Intn(3)
		switch op {
		case 0:
			v := uint64(rng.Intn(5000))
			if err := idx.Insert(ctx, k, v); err != nil {
				t.Fatalf("insert step %d: %v", step, err)
			}
			model[k] = v
		case 1:
			if err := idx.Delete(ctx, k); err != nil {
				t.Fatalf("delete step %d: %v", step, err)
			}
			delete(model, k)
		case 2:
			v, ok, err := idx.Search(ctx, k)
			if err != nil {
				t.Fatalf("search step %d: %v", step, err)
			}
			mv, mok := model[k]
			if ok != mok || (ok && v != mv) {
				t.Fatalf("model mismatch at step %d key %d", step, k)
			}
		}
		if step%40 == 0 {
			assertTreeInvariants(t, idx, ctx)
		}
	}
	keys := make([]int, 0, len(model))
	for k := range model {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	it, err := idx.RangeScan(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		e, ok := it.Next()
		if !ok || e.Key != uint64(k) || e.Value != model[uint64(k)] {
			t.Fatalf("iterator mismatch at key %d", k)
		}
	}
	if _, ok := it.Next(); ok {
		t.Fatalf("iterator has extra entries")
	}
}

func TestConcurrentMutationSafety(t *testing.T) {
	idx := New(newMemTxnClient(), Config{MemID: 1, MaxRetries: 80, RetryBackoff: time.Microsecond})
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				k := uint64((w*17 + i) % 64)
				if i%3 == 0 {
					_ = idx.Delete(ctx, k)
				} else {
					_ = idx.Insert(ctx, k, uint64(w*1000+i))
				}
			}
		}()
	}
	wg.Wait()
	assertTreeInvariants(t, idx, ctx)
}

type conflictInjectingClient struct {
	base         *memTxnClient
	rejectFirstN int32
	calls        int32
}

func (c *conflictInjectingClient) Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error) {
	return c.base.Read(ctx, memID, addr, length)
}

func (c *conflictInjectingClient) Write(ctx context.Context, memID uint32, addr uint64, data []byte) error {
	return c.base.Write(ctx, memID, addr, data)
}

func (c *conflictInjectingClient) ExecAndCommit(ctx context.Context, class string, cmp []sinfoniaclient.CmpItem, reads []sinfoniaclient.ReadItem, writes []sinfoniaclient.WriteItem) sinfoniaclient.ExecResult {
	_ = class
	_ = reads
	call := atomic.AddInt32(&c.calls, 1)
	if call <= atomic.LoadInt32(&c.rejectFirstN) {
		return sinfoniaclient.ExecResult{Committed: false, ReadResults: map[sinfoniaclient.ReadKey][]byte{}}
	}
	return c.base.ExecAndCommit(ctx, class, cmp, reads, writes)
}

func TestInsertRetriesConflictAndEventuallyCommits(t *testing.T) {
	client := &conflictInjectingClient{base: newMemTxnClient(), rejectFirstN: 3}
	idx := New(client, Config{MemID: 1, MaxRetries: 10, RetryBackoff: 10 * time.Microsecond})
	ctx := context.Background()
	if err := idx.Insert(ctx, 42, 4200); err != nil {
		t.Fatalf("insert with retry: %v", err)
	}
	if atomic.LoadInt32(&client.calls) < 4 {
		t.Fatalf("expected retries, got calls=%d", atomic.LoadInt32(&client.calls))
	}
	v, ok, err := idx.Search(ctx, 42)
	if err != nil || !ok || v != 4200 {
		t.Fatalf("search after retry insert => v=%d ok=%v err=%v", v, ok, err)
	}
}

func TestInsertExhaustsRetryBudget(t *testing.T) {
	client := &conflictInjectingClient{base: newMemTxnClient(), rejectFirstN: 100}
	idx := New(client, Config{MemID: 1, MaxRetries: 4, RetryBackoff: 10 * time.Microsecond})
	ctx := context.Background()
	err := idx.Insert(ctx, 5, 50)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("insert err=%v want=%v", err, ErrBusy)
	}
	if got := atomic.LoadInt32(&client.calls); got != 4 {
		t.Fatalf("commit attempts=%d want=4", got)
	}
}

func TestDeleteRebalanceRootCollapseAndEmptyTree(t *testing.T) {
	idx := New(newMemTxnClient(), Config{MemID: 1, MaxRetries: 20, RetryBackoff: time.Microsecond})
	ctx := context.Background()
	for i := uint64(1); i <= 64; i++ {
		if err := idx.Insert(ctx, i, i*100); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	assertTreeInvariants(t, idx, ctx)
	for i := uint64(1); i <= 64; i++ {
		if err := idx.Delete(ctx, i); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
		if i%8 == 0 {
			assertTreeInvariants(t, idx, ctx)
		}
	}
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if s.super.RootPage != 0 {
		t.Fatalf("expected empty tree root=0 got %d", s.super.RootPage)
	}
}

func assertTreeInvariants(t *testing.T, idx *Index, ctx context.Context) {
	t.Helper()
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if s.super.RootPage == 0 {
		return
	}
	seenLeaves := map[uint32]bool{}
	var walk func(id uint32, isRoot bool) (min, max uint64)
	walk = func(id uint32, isRoot bool) (uint64, uint64) {
		n := s.nodes[id]
		if n.Leaf {
			if len(n.Keys) != len(n.Values) {
				t.Fatalf("leaf key/value mismatch")
			}
			if !isRoot && len(n.Keys) < minLeafKeys() {
				t.Fatalf("leaf underflow")
			}
			if len(n.Keys) > maxLeafKeys {
				t.Fatalf("leaf overflow")
			}
			for i := 1; i < len(n.Keys); i++ {
				if n.Keys[i-1] >= n.Keys[i] {
					t.Fatalf("leaf keys not strict ascending")
				}
			}
			seenLeaves[id] = true
			if len(n.Keys) == 0 {
				return 0, 0
			}
			return n.Keys[0], n.Keys[len(n.Keys)-1]
		}
		if len(n.Children) != len(n.Keys)+1 {
			t.Fatalf("internal child/key mismatch")
		}
		if !isRoot && len(n.Keys) < minInternalKeys() {
			t.Fatalf("internal underflow")
		}
		if len(n.Keys) > maxInternalKeys {
			t.Fatalf("internal overflow")
		}
		cmin, cmax := walk(n.Children[0], false)
		_ = cmin
		minv := cmin
		maxv := cmax
		for i := 1; i < len(n.Children); i++ {
			nmin, nmax := walk(n.Children[i], false)
			if cmax >= nmin {
				t.Fatalf("child key ranges overlap")
			}
			cmax = nmax
			maxv = nmax
		}
		return minv, maxv
	}
	walk(s.super.RootPage, true)

	leaf := s.nodes[s.super.RootPage]
	for !leaf.Leaf {
		leaf = s.nodes[leaf.Children[0]]
	}
	prev := uint64(0)
	first := true
	count := 0
	for {
		if seenLeaves[leaf.ID] == false {
			t.Fatalf("leaf chain references unknown leaf")
		}
		for _, k := range leaf.Keys {
			if !first && k <= prev {
				t.Fatalf("leaf chain order violated")
			}
			first = false
			prev = k
			count++
		}
		if leaf.NextLeaf == 0 {
			break
		}
		leaf = s.nodes[leaf.NextLeaf]
	}
	_ = count
}
