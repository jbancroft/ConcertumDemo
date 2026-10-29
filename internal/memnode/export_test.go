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
