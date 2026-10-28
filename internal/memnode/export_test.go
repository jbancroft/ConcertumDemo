package memnode

// ApplyWriteForTest changes memory and disk directly, without transaction logs,
// so tests can set up the state they need. This file is only built by go test.
func (n *Participant) ApplyWriteForTest(addr uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.write(addr, data)
}
