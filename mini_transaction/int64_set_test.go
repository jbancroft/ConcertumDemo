package mini_transaction

import (
	"math"
	"slices"
	"testing"
)

// The membership cases retain gofonia's TestInt64Set, with iteration and ID
// range checks added for its use in coordinator participant discovery.
func TestInt64Set(t *testing.T) {
	set := NewInt64Set()
	if set.Len() != 0 {
		t.Fatal("new set is not empty")
	}
	set.Add(1)
	set.Add(2)
	set.Add(1)
	if set.Len() != 2 {
		t.Fatal("duplicate host ID was retained")
	}
	set.Remove(1)
	set.Remove(2)
	if set.Len() != 0 {
		t.Fatal("removed host IDs remain in the set")
	}
	set.Add(math.MinInt64)
	set.Add(math.MaxInt64)
	values := set.ToInt64Slice()
	slices.Sort(values)
	if !slices.Equal(values, []int64{math.MinInt64, math.MaxInt64}) {
		t.Fatalf("set values=%v", values)
	}
	visited := 0
	set.Each(func(int64) bool { visited++; return false })
	if visited != 1 {
		t.Fatalf("early-stop iterator visited %d values", visited)
	}
}
