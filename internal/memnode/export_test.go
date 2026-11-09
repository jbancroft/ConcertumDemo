package memnode

import (
	"os"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func newTestParticipant(t testing.TB, id uint32, size int) *Participant {
	t.Helper()
	node, err := NewParticipant(id, size, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if err := node.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	return node
}

func (n *Participant) SetForcedAbortFsyncHookForTest(fn func(*os.File) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.forcedAborts.SetFsyncHookForTest(fn)
}

func (n *Participant) SetDecisionSyncDirHookForTest(fn func(string) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.decided.SetSyncDirHookForTest(fn)
}

func (n *Participant) SetClockForTest(now func() time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nowFn = now
}

// ApplyWriteForTest changes memory and disk directly, without transaction logs,
// so tests can set up the state they need. This file is only built by go test.
func (n *Participant) ApplyWriteForTest(addr uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applyWrites([]sinfonia.WriteItem{{Addr: addr, Data: data}})
}

// ReadBytesForTest inspects physical memory, including unready nodes.
// It exists only in test builds; applications must use minitransactions.
func (n *Participant) ReadBytesForTest(addr uint64, length uint32) ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.read(addr, length)
}

func (n *Participant) ActiveLockCountForTest() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.locks.locks)
}

func (n *Participant) ReadyForTest() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.restartRequired == nil && n.recoveryComplete
}

// SetWriteApplyFaultForTest lets recovery tests fail a write after memory changes
// but before the disk image is updated.
func (n *Participant) SetWriteApplyFaultForTest(fn func(addr uint64, data []byte) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.writeApplyFaultHook = fn
}

func (n *Participant) SetRedoFsyncHookForTest(fn func(*os.File) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.redoLog.SetFsyncHookForTest(fn)
}

func (n *Participant) HasAppliedTrackingForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.appliedLocal[txId] {
		return true
	}
	if rem := n.appliedRemote[txId]; len(rem) > 0 {
		return true
	}
	for _, txIds := range n.pendingNotify {
		if _, ok := txIds[txId]; ok {
			return true
		}
	}
	return false
}

func (n *Participant) SetRecoverBoundaryHookForTest(fn func(phase string, txId sinfonia.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recoverBoundaryHook = fn
}

func (n *Participant) HasDecidedForTest(txId sinfonia.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.decided.Get(txId)
	return ok
}

func (n *Participant) ForceAbortForTest(txId sinfonia.TransactionID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.forcedAborts.AddItem(txId)
}
