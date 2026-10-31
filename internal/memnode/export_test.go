package memnode

import "time"

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
	return n.write(addr, data)
}

// ReadBytesForTest inspects physical memory, including unready nodes and replicas.
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
