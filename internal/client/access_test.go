package client

import "context"

func readParticipant(ctx context.Context, node Participant, addr uint64, length uint32) ([]byte, error) {
	c := NewCoordinator(map[uint32]Participant{1: node})
	c.SetMaxRetriesForTest(0)
	return c.Read(ctx, 1, addr, length)
}
