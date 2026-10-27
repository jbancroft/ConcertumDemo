package memnode

import "testing"

// The zero-length, same-offset, overlapping, and disjoint cases come from
// gofonia/mutex/range_lock_test.go, using the retained endpoint representation.
func TestRangeLockOverlapsWith(t *testing.T) {
	for _, test := range []struct {
		name          string
		first, second RangeLock
		want          bool
	}{
		{"zero length", RangeLock{}, RangeLock{}, false},
		{"empty inside another range", RangeLock{start: 4, end: 4}, RangeLock{start: 3, end: 8}, false},
		{"same offset", RangeLock{start: 3, end: 4}, RangeLock{start: 3, end: 8}, true},
		{"different offset", RangeLock{start: 4, end: 5}, RangeLock{start: 3, end: 8}, true},
		{"disjoint", RangeLock{start: 4, end: 7}, RangeLock{start: 1, end: 3}, false},
		{"adjacent", RangeLock{start: 1, end: 3}, RangeLock{start: 3, end: 7}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.first.OverlapsWith(test.second); got != test.want {
				t.Fatalf("overlap=%t, want %t", got, test.want)
			}
			if got := test.second.OverlapsWith(test.first); got != test.want {
				t.Fatalf("reverse overlap=%t, want %t", got, test.want)
			}
		})
	}
}

func TestLockManagerChecksEarlierLongReadLocks(t *testing.T) {
	manager := LockManager{}
	if !manager.tryLock(0, 100, false) || !manager.tryLock(10, 20, false) {
		t.Fatal("compatible nested readers were rejected")
	}
	// The short reader ends before the requested write, but the earlier long
	// reader still overlaps. A scan must not stop at the short reader.
	if ok, conflict := manager.tryLockWithConflict(50, 60, true); ok || conflict == nil || conflict.HeldStart != 0 || conflict.HeldEnd != 100 {
		t.Fatalf("write bypassed the long reader: ok=%t conflict=%+v", ok, conflict)
	}
	manager.unlock(RangeLock{start: 0, end: 100})
	if !manager.tryLock(50, 60, true) {
		t.Fatal("write remained blocked after the overlapping reader was released")
	}
}
