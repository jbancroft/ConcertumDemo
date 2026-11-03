package sinfoniaclient

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

const defaultNodeTimeout = 5 * time.Second

type ReadKey = minitxn.ReadKey
type Status = minitxn.Status

const (
	StatusAborted   = minitxn.StatusAborted
	StatusCommitted = minitxn.StatusCommitted
	StatusUnknown   = minitxn.StatusUnknown
)

// ErrTransactionAborted reports a definite rejection of a Read or Write call.
var ErrTransactionAborted = client.ErrTransactionAborted

// ErrUnknownOutcome reports an uncertain transaction result. The operation may
// have committed; callers must not retry writes blindly.
var ErrUnknownOutcome = client.ErrUnknownOutcome

type CompareItem = minitxn.CompareItem
type ReadItem = minitxn.ReadItem
type WriteItem = minitxn.WriteItem
type ExecResult = minitxn.ExecResult

type MiniTransaction = minitxn.MiniTransaction

// TxnClient is the read/write and minitransaction interface used by the index
// and SQL layers.
type TxnClient interface {
	ExecuteAndCommit(ctx context.Context, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
	Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
}

// Client coordinates minitransactions across configured memory nodes.
type Client struct {
	coordinator *client.Coordinator
	nodes       []*client.RPCParticipant
	closeOnce   sync.Once
	closeErr    error
}

type NodeConfig struct {
	RPCAddress string
}

// NewClient configures the coordinator from explicit memory node addresses.
func NewClient(config map[uint32]NodeConfig) *Client {
	c := &Client{}
	nodes := make(map[uint32]client.Participant, len(config))
	for id, node := range config {
		participant := client.NewRPCParticipant(node.RPCAddress, defaultNodeTimeout)
		nodes[id] = participant
		c.nodes = append(c.nodes, participant)
	}
	coord := client.NewCoordinator(nodes)
	c.coordinator = coord
	return c
}

// Close drains queued decisions and releases workers and RPC connections.
// Call after all client calls have returned. Only repeated Close calls are allowed
// afterward. Delivery errors do not undo a transaction's known decision.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.coordinator.Close()
		for _, node := range c.nodes {
			c.closeErr = errors.Join(c.closeErr, node.Close())
		}
	})
	return c.closeErr
}

// Read executes a read-only minitransaction. Separate calls are separate snapshots.
func (c *Client) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return c.coordinator.Read(ctx, hostId, addr, length)
}

// Write executes a write-only minitransaction on a memory node.
// ErrUnknownOutcome means the write may have committed.
func (c *Client) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return c.coordinator.Write(ctx, hostId, addr, data)
}

func (c *Client) SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error {
	return c.coordinator.SetRetryPolicy(maxRetries, baseBackoff, maxBackoff)
}

// FlushPendingCommits waits for queued commit/abort messages to reach the nodes.
// ExecuteAndCommit can return once the decision is recoverable, before those
// messages finish. Flush when you need to wait for delivery and check its errors.
func (c *Client) FlushPendingCommits(ctx context.Context) error {
	return c.coordinator.FlushPendingCommits(ctx)
}

// ExecuteAndCommit submits the operations as one minitransaction.
func (c *Client) ExecuteAndCommit(ctx context.Context, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
	internalCmp := make([]sinfonia.CmpItem, 0, len(compares))
	for _, item := range compares {
		internalCmp = append(internalCmp, sinfonia.CmpItem{MemID: item.HostId, Addr: item.Address, Data: append([]byte(nil), item.Data...)})
	}
	internalReads := make([]sinfonia.ReadItem, 0, len(reads))
	for _, item := range reads {
		internalReads = append(internalReads, sinfonia.ReadItem{MemID: item.HostId, Addr: item.Address, Len: item.Length})
	}
	internalWrites := make([]sinfonia.WriteItem, 0, len(writes))
	for _, item := range writes {
		internalWrites = append(internalWrites, sinfonia.WriteItem{MemID: item.HostId, Addr: item.Address, Data: append([]byte(nil), item.Data...)})
	}
	res := c.coordinator.ExecuteAndCommit(ctx, internalCmp, internalReads, internalWrites)
	return publicResult(res)
}

func publicResult(res client.ExecResult) ExecResult {
	readsOut := make(map[ReadKey][]byte, len(res.ReadResults))
	for k, v := range res.ReadResults {
		readsOut[ReadKey{HostId: k.MemID, Address: k.Addr, Length: k.Len}] = append([]byte(nil), v...)
	}
	votes := map[uint32]int{}
	for hostId, vote := range res.ParticipantVotes {
		votes[hostId] = int(vote)
	}
	if res.UnknownOutcome {
		return ExecResult{Err: res.TransportErr, Committed: false, UnknownOutcome: true, ReadResults: map[ReadKey][]byte{}, Status: StatusUnknown, ParticipantVotes: votes}
	}
	if !res.Committed {
		return ExecResult{Err: res.TransportErr, Committed: false, ReadResults: map[ReadKey][]byte{}, Status: StatusAborted, ParticipantVotes: votes}
	}
	return ExecResult{Err: res.TransportErr, Committed: true, ReadResults: readsOut, Status: StatusCommitted, ParticipantVotes: votes}
}

func (c *Client) NewMiniTransaction() *MiniTransaction {
	return minitxn.NewMiniTransaction(c)
}
