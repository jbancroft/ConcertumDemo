package memnode

import (
	"slices"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

type RangeLock struct {
	start uint64
	end   uint64
	write bool
}

// OverlapsWith checks [start, end) ranges: touching endpoints don't overlap.
// This follows the range comparison from gofonia.
func (rl RangeLock) OverlapsWith(other RangeLock) bool {
	if rl.start >= rl.end || other.start >= other.end {
		return false
	}
	minEnd, maxOffset := rl.end, other.start
	if rl.start > other.start {
		minEnd, maxOffset = other.end, rl.start
	}
	return minEnd > maxOffset
}

type LockManager struct {
	locks []RangeLock
}

// tryLockRanges acquires the whole transaction's lock set or leaves it untouched.
// Check only existing locks: overlaps within this request belong to one transaction.
// The caller must hold the participant mutex through both checking and insertion.
func (m *LockManager) tryLockRanges(requested []RangeLock) *sinfonia.LockConflict {
	for _, l := range requested {
		if conflict := m.findConflict(l.start, l.end, l.write); conflict != nil {
			return conflict
		}
	}
	for _, l := range requested {
		idx := lowerBoundLock(m.locks, l.start)
		m.locks = append(m.locks, RangeLock{})
		copy(m.locks[idx+1:], m.locks[idx:])
		m.locks[idx] = l
	}
	return nil
}

func (m *LockManager) findConflict(start, end uint64, write bool) *sinfonia.LockConflict {
	idx := lowerBoundLock(m.locks, start)
	for i := idx; i < len(m.locks); i++ {
		l := m.locks[i]
		if l.start >= end {
			break
		}
		if !rangesOverlap(start, end, l.start, l.end) {
			continue
		}
		if write || l.write {
			return &sinfonia.LockConflict{
				Reason:         "range_overlap",
				RequestedStart: start, RequestedEnd: end, RequestedWrite: write,
				HeldStart: l.start, HeldEnd: l.end, HeldWrite: l.write,
			}
		}
	}
	for i := idx - 1; i >= 0; i-- {
		l := m.locks[i]
		if l.end <= start {
			continue
		}
		if rangesOverlap(start, end, l.start, l.end) && (write || l.write) {
			return &sinfonia.LockConflict{
				Reason:         "range_overlap",
				RequestedStart: start, RequestedEnd: end, RequestedWrite: write,
				HeldStart: l.start, HeldEnd: l.end, HeldWrite: l.write,
			}
		}
	}
	return nil
}

func lowerBoundLock(locks []RangeLock, start uint64) int {
	lo, hi := 0, len(locks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if locks[mid].start < start {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (m *LockManager) unlock(target RangeLock) {
	for i := range m.locks {
		l := m.locks[i]
		if l.start == target.start && l.end == target.end && l.write == target.write {
			m.locks = append(m.locks[:i], m.locks[i+1:]...)
			return
		}
	}
}

func rangesOverlap(aStart, aEnd, bStart, bEnd uint64) bool {
	return (RangeLock{start: aStart, end: aEnd}).OverlapsWith(RangeLock{start: bStart, end: bEnd})
}

// tryTransactionLocksLocked protects every nonempty item, including the parts of
// reads and comparisons that extend beyond this transaction's own writes.
func (n *Participant) tryTransactionLocksLocked(compares []sinfonia.CmpItem, reads []sinfonia.ReadItem, writes []sinfonia.WriteItem) ([]RangeLock, *sinfonia.LockConflict) {
	requested := make([]RangeLock, 0, len(compares)+len(reads)+len(writes))
	add := func(item minitxn.RangeItem, write bool) {
		if item.Size() != 0 {
			requested = append(requested, RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: write})
		}
	}
	for _, item := range writes {
		add(item, true)
	}
	for _, item := range compares {
		add(item, false)
	}
	for _, item := range reads {
		add(item, false)
	}
	if conflict := n.locks.tryLockRanges(requested); conflict != nil {
		return nil, conflict
	}
	return requested, nil
}

func (n *Participant) releaseLocks(locks []RangeLock) {
	for _, lock := range slices.Backward(locks) {
		n.locks.unlock(lock)
	}
}
