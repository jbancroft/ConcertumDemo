package memnode

// ApplyWriteForTest bypasses the public direct-write safety boundary so tests
// can arrange exact on-disk and in-memory states without adding transaction-log
// entries that would change the behavior under test. It is not part of the
// production memnode API because this file is compiled only by go test.
func (n *Node) ApplyWriteForTest(addr uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.write(addr, data)
}
