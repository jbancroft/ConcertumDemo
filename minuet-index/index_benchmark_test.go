package minuetindex

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	benchDatasetLarge          = 1024
	benchDatasetMedium         = 512
	benchDatasetSmall          = 256
	benchRangeSpanNarrow       = 64
	benchRangeSpanWide         = 512
	benchSeedPrimary     int64 = 20260414
	benchSeedSplitHeavy  int64 = 20260415
)

func benchmarkIndex(b *testing.B) (*BTree, context.Context) {
	b.Helper()
	idx := newTestBTree(b, newMemTxnClient(), Config{HostId: 1, MaxRetries: 20, RetryBackoff: time.Microsecond})
	return idx, b.Context()
}

func preloadSequential(b *testing.B, idx *BTree, ctx context.Context, n int) {
	b.Helper()
	for i := range n {
		if err := idx.Insert(ctx, uint64(i), uint64(i)); err != nil {
			b.Fatalf("preload insert %d: %v", i, err)
		}
	}
}

func BenchmarkMinuetIndexSearch(b *testing.B) {
	for _, datasetSize := range []int{benchDatasetSmall, benchDatasetLarge} {
		b.Run(fmt.Sprintf("dataset_%d", datasetSize), func(b *testing.B) {
			idx, ctx := benchmarkIndex(b)
			preloadSequential(b, idx, ctx, datasetSize)
			rng := rand.New(rand.NewSource(benchSeedPrimary))
			for b.Loop() {
				key := uint64(rng.Intn(datasetSize))
				if _, ok, err := idx.Search(ctx, key); err != nil {
					b.Fatalf("search err: %v", err)
				} else if !ok {
					b.Fatalf("search key missing: %d", key)
				}
			}
		})
	}
}

func BenchmarkMinuetIndexInsert(b *testing.B) {
	b.Run("randomized", func(b *testing.B) {
		idx, ctx := benchmarkIndex(b)
		rng := rand.New(rand.NewSource(benchSeedPrimary))
		inserts := 0
		var value uint64
		for b.Loop() {
			if inserts == benchDatasetLarge {
				b.StopTimer()
				idx, ctx = benchmarkIndex(b)
				inserts = 0
				b.StartTimer()
			}
			key := uint64(rng.Int63())
			if err := idx.Insert(ctx, key, value); err != nil {
				b.Fatalf("insert err: %v", err)
			}
			inserts++
			value++
		}
	})

	b.Run("split_heavy_ascending", func(b *testing.B) {
		idx, ctx := benchmarkIndex(b)
		rng := rand.New(rand.NewSource(benchSeedSplitHeavy))
		base := rng.Uint64() &^ uint64(0xffff)
		inserts := 0
		for b.Loop() {
			if inserts == benchDatasetLarge {
				b.StopTimer()
				idx, ctx = benchmarkIndex(b)
				inserts = 0
				b.StartTimer()
			}
			key := base + uint64(inserts)
			if err := idx.Insert(ctx, key, key); err != nil {
				b.Fatalf("insert err: %v", err)
			}
			inserts++
		}
	})
}

func BenchmarkMinuetIndexDelete(b *testing.B) {
	b.Run("uniform", func(b *testing.B) {
		idx, ctx := benchmarkIndex(b)
		preloadSequential(b, idx, ctx, benchDatasetMedium)
		next := 0
		for b.Loop() {
			k := uint64(next % benchDatasetMedium)
			if err := idx.Delete(ctx, k); err != nil {
				b.Fatalf("delete err: %v", err)
			}
			if err := idx.Insert(ctx, k, k); err != nil {
				b.Fatalf("reinsert err: %v", err)
			}
			next++
		}
	})

	b.Run("merge_heavy_descending", func(b *testing.B) {
		idx, ctx := benchmarkIndex(b)
		preloadSequential(b, idx, ctx, benchDatasetMedium)
		remaining := benchDatasetMedium
		for b.Loop() {
			if remaining == 0 {
				b.StopTimer()
				idx, ctx = benchmarkIndex(b)
				preloadSequential(b, idx, ctx, benchDatasetMedium)
				remaining = benchDatasetMedium
				b.StartTimer()
			}
			k := uint64(remaining - 1)
			if err := idx.Delete(ctx, k); err != nil {
				b.Fatalf("delete err: %v", err)
			}
			remaining--
		}
	})
}

func BenchmarkMinuetIndexRangeScan(b *testing.B) {
	for _, tc := range []struct {
		name        string
		datasetSize int
		span        int
	}{
		{name: "narrow_span", datasetSize: benchDatasetLarge, span: benchRangeSpanNarrow},
		{name: "wide_span", datasetSize: benchDatasetLarge, span: benchRangeSpanWide},
	} {
		b.Run(tc.name, func(b *testing.B) {
			idx, ctx := benchmarkIndex(b)
			preloadSequential(b, idx, ctx, tc.datasetSize)
			rng := rand.New(rand.NewSource(benchSeedPrimary))
			for b.Loop() {
				start := rng.Intn(tc.datasetSize - tc.span)
				it, err := idx.RangeScan(ctx, uint64(start), uint64(start+tc.span-1))
				if err != nil {
					b.Fatalf("rangescan err: %v", err)
				}
				count := 0
				for {
					_, ok := it.Next()
					if !ok {
						break
					}
					count++
				}
				if count != tc.span {
					b.Fatalf("rangescan count=%d want=%d", count, tc.span)
				}
			}
		})
	}
}
