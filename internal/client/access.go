package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

// Read executes a single read item through the ordinary transaction path.
// Separate calls do not form a shared snapshot.
func (c *Coordinator) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	item := sinfonia.ReadItem{MemID: hostId, Addr: addr, Len: length}
	res := c.ExecuteAndCommit(ctx, "", nil, []sinfonia.ReadItem{item}, nil)
	if err := accessError(res); err != nil {
		return nil, err
	}
	data, ok := res.ReadResults[item]
	if !ok || uint64(len(data)) != uint64(length) {
		return nil, errors.New("sinfonia: transaction returned incomplete read data")
	}
	return data, nil
}

// Write executes a single write item, including locking and durable logging.
func (c *Coordinator) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	item := sinfonia.WriteItem{MemID: hostId, Addr: addr, Data: append([]byte(nil), data...)}
	return accessError(c.ExecuteAndCommit(ctx, "", nil, nil, []sinfonia.WriteItem{item}))
}

func accessError(res ExecResult) error {
	if res.UnknownOutcome {
		return &minitxn.UnknownOutcomeError{Handle: res.Recovery, Cause: res.TransportErr}
	}
	if !res.Committed {
		return fmt.Errorf("%w: vote %v", ErrTransactionAborted, res.Status)
	}
	return res.TransportErr
}
