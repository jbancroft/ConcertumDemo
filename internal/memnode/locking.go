package memnode

import (
	"context"
	"errors"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

var ErrBackupLockLease = errors.New("backup global lock lease is invalid")

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
	locks                 []RangeLock
	globalWriteLock       bool
	globalOwner           string
	globalLeaseUntil      time.Time
	globalLeaseGeneration uint64
}

func (m *LockManager) tryLock(start, end uint64, write bool) bool {
	ok, _ := m.tryLockWithConflict(start, end, write)
	return ok
}

func (m *LockManager) tryLockWithConflict(start, end uint64, write bool) (bool, *sinfonia.LockConflict) {
	conflict := m.tryLockRanges([]RangeLock{{start: start, end: end, write: write}})
	return conflict == nil, conflict
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
	if m.globalWriteLock {
		return &sinfonia.LockConflict{Reason: "global_write_lock", RequestedStart: start, RequestedEnd: end, RequestedWrite: write}
	}
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
	for i := len(locks) - 1; i >= 0; i-- {
		n.locks.unlock(locks[i])
	}
	n.cond.Broadcast()
}

func (n *Participant) LockAllBlocking(ctx context.Context) error {
	return n.LockAllBlockingOwned(ctx, n.localBarrierOwner)
}

func (n *Participant) LockAllBlockingOwned(ctx context.Context, owner string) error {
	if owner == "" {
		return ErrBackupLockLease
	}
	// sync.Cond doesn't understand contexts. Wake it on cancellation; the timer
	// below also wakes it when an abandoned owner's lease expires.
	wakeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.cond.Broadcast()
			n.mu.Unlock()
		case <-wakeDone:
		}
	}()
	defer close(wakeDone)

	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if n.restartRequired != nil {
			return n.restartRequired
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		n.expireGlobalLockLocked()
		if !n.locks.globalWriteLock && len(n.locks.locks) == 0 {
			n.locks.globalWriteLock = true
			n.locks.globalOwner = owner
			n.locks.globalLeaseGeneration++
			n.renewGlobalLeaseLocked()
			return nil
		}
		n.cond.Wait()
	}
}

func (n *Participant) UnlockAll() {
	n.UnlockAllOwned(n.localBarrierOwner)
}

func (n *Participant) UnlockAllOwned(owner string) {
	if owner == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.expireGlobalLockLocked()
	if n.locks.globalOwner != owner {
		return
	}
	n.clearGlobalLockLocked()
	n.cond.Broadcast()
}

func (n *Participant) RenewAllOwned(ctx context.Context, owner string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.touchGlobalLockLocked(owner); err != nil {
		return err
	}
	return nil
}

func (n *Participant) expireGlobalLockLocked() {
	if !n.locks.globalWriteLock || n.locks.globalLeaseUntil.IsZero() {
		return
	}
	if time.Now().Before(n.locks.globalLeaseUntil) {
		return
	}
	n.clearGlobalLockLocked()
	n.cond.Broadcast()
}

func (n *Participant) clearGlobalLockLocked() {

	n.locks.globalWriteLock = false
	n.locks.globalOwner = ""
	n.locks.globalLeaseUntil = time.Time{}
}

func (n *Participant) touchGlobalLockLocked(owner string) error {
	if n.restartRequired != nil {
		return n.restartRequired
	}
	n.expireGlobalLockLocked()
	if owner == "" || !n.locks.globalWriteLock || n.locks.globalOwner != owner {
		return ErrBackupLockLease
	}
	n.renewGlobalLeaseLocked()
	return nil
}

func (n *Participant) renewGlobalLeaseLocked() {
	ttl := n.globalBarrierLeaseTTL
	if ttl <= 0 {
		ttl = defaultGlobalBarrierLeaseTTL
	}
	n.locks.globalLeaseUntil = time.Now().Add(ttl)
	generation := n.locks.globalLeaseGeneration
	owner := n.locks.globalOwner
	time.AfterFunc(ttl, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.locks.globalWriteLock && n.locks.globalOwner == owner && n.locks.globalLeaseGeneration == generation {
			n.expireGlobalLockLocked()
		}
	})
}

func (n *Participant) HoldWriteLockForTest(addr uint64, length uint32) func() {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := RangeLock{start: addr, end: addr + uint64(length), write: true}
	if !n.locks.tryLock(l.start, l.end, true) {
		return func() {}
	}
	return func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.locks.unlock(l)
	}
}
