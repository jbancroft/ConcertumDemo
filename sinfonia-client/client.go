package sinfoniaclient

import (
	"context"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

const defaultNodeTimeout = 5 * time.Second

type ReadKey = minitxn.ReadKey
type Status = minitxn.Status

const (
	StatusUnknown   = minitxn.StatusUnknown
	StatusCommitted = minitxn.StatusCommitted
	StatusAborted   = minitxn.StatusAborted
)

// ErrUnknownOutcome reports an uncertain transaction result. The operation may
// have committed; callers must not retry writes blindly.
var ErrUnknownOutcome = client.ErrUnknownOutcome

// ErrTransactionAborted reports a definite rejection of a Read or Write call.
var ErrTransactionAborted = client.ErrTransactionAborted

type CompareItem = minitxn.CompareItem
type ReadItem = minitxn.ReadItem
type WriteItem = minitxn.WriteItem
type ExecResult = minitxn.ExecResult
type RecoveryHandle = minitxn.RecoveryHandle
type UnknownOutcomeError = minitxn.UnknownOutcomeError

var ErrOutcomeExpired = minitxn.ErrOutcomeExpired
var ErrReadResultsUnavailable = minitxn.ErrReadResultsUnavailable

type MiniTransaction = minitxn.MiniTransaction

// TxnClient is the read/write and minitransaction interface used by the index
// and SQL layers.
type TxnClient interface {
	ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
	Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
}

// Client coordinates minitransactions across configured memory nodes.
type Client struct {
	coordinator *client.Coordinator
}

type NodeConfig struct {
	RPCAddress string
	Generation uint64 // Fencing generation expected by the node.
}

// NewClient configures the coordinator from explicit memory node addresses.
func NewClient(config map[uint32]NodeConfig) *Client {
	nodes := make(map[uint32]client.Participant, len(config))
	generations := make(map[uint32]uint64, len(config))
	for id, node := range config {
		nodes[id] = client.NewRPCParticipantWithGeneration(node.RPCAddress, nil, defaultNodeTimeout, node.Generation)
		generations[id] = node.Generation
	}
	coord := client.NewCoordinator(nodes)
	coord.SetGenerations(generations)
	return &Client{coordinator: coord}
}

// Read executes a read-only minitransaction. Separate calls are separate snapshots.
func (c *Client) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return c.coordinator.Read(ctx, hostId, addr, length)
}

// Write executes a write-only minitransaction on either a RAM or durable node.
// ErrUnknownOutcome means the write may have committed.
func (c *Client) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return c.coordinator.Write(ctx, hostId, addr, data)
}

// Resolve checks an uncertain attempt using its original participant evidence.
func (c *Client) Resolve(ctx context.Context, handle RecoveryHandle) ExecResult {
	return publicResult(c.coordinator.Resolve(ctx, handle))
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
func (c *Client) ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
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
	res := c.coordinator.ExecuteAndCommit(ctx, class, internalCmp, internalReads, internalWrites)
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
		return ExecResult{Recovery: res.Recovery, Err: res.TransportErr, Committed: false, UnknownOutcome: true, ReadResults: map[ReadKey][]byte{}, Status: StatusUnknown, ParticipantVotes: votes}
	}
	if !res.Committed {
		return ExecResult{Recovery: res.Recovery, Err: res.TransportErr, Committed: false, ReadResults: map[ReadKey][]byte{}, Status: StatusAborted, ParticipantVotes: votes}
	}
	return ExecResult{Recovery: res.Recovery, Err: res.TransportErr, Committed: true, ReadResults: readsOut, Status: StatusCommitted, ParticipantVotes: votes}
}

func (c *Client) NewMiniTransaction() *MiniTransaction {
	return minitxn.NewMiniTransaction(c)
}
