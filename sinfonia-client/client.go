package sinfoniaclient

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

type ReadKey = minitxn.ReadKey
type Status = minitxn.Status

const (
	StatusUnknown   = minitxn.StatusUnknown
	StatusCommitted = minitxn.StatusCommitted
	StatusAborted   = minitxn.StatusAborted
)

type CompareItem = minitxn.CompareItem
type ReadItem = minitxn.ReadItem
type WriteItem = minitxn.WriteItem
type ExecResult = minitxn.ExecResult
type MiniTransaction = minitxn.MiniTransaction

type RateWindows struct {
	Short  float64 `json:"short"`
	Medium float64 `json:"medium"`
	Long   float64 `json:"long"`
}

type TxnRateGroup struct {
	ExecutedPerSec RateWindows `json:"executed_per_sec"`
	RetriedPerSec  RateWindows `json:"retried_per_sec"`
	AbortedPerSec  RateWindows `json:"aborted_per_sec"`
}

type TxnClassStats struct {
	Total     uint64       `json:"total"`
	Committed uint64       `json:"committed"`
	Aborted   uint64       `json:"aborted"`
	Retried   uint64       `json:"retried"`
	Rates     TxnRateGroup `json:"rates"`
}

type Stats struct {
	NodeID uint32 `json:"node_id"`
	Role   string `json:"role"`
	Txns   struct {
		Total     uint64 `json:"total"`
		Committed uint64 `json:"committed"`
		Aborted   uint64 `json:"aborted"`
		Retried   uint64 `json:"retried"`
	} `json:"txns"`
	Rates struct {
		Txns               TxnRateGroup `json:"txns"`
		BytesWrittenPerSec RateWindows  `json:"bytes_written_per_sec"`
	} `json:"rates"`
	ByClass map[string]TxnClassStats `json:"by_class,omitempty"`
}

// TxnClient is the read/write and minitransaction interface used by the index
// and SQL layers.
type TxnClient interface {
	ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
	Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
}

// Backend supplies the reads, writes, stats, and transactions used by Client.
type Backend interface {
	Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
	GetStats(hostId uint32) (Stats, error)
	ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
}

// Client sends operations to its transaction backend.
type Client struct {
	backend Backend
}

func NewClient(backend any) *Client {
	switch b := backend.(type) {
	case Backend:
		return &Client{backend: b}
	case interface {
		Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
		Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
		GetStats(hostId uint32) (memnode.StatsResponse, error)
		ExecuteAndCommit(ctx context.Context, class string, compares []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) client.ExecResult
	}:
		return &Client{backend: &coordinatorBackend{inner: b}}
	default:
		panic("sinfoniaclient.NewClient: unsupported backend")
	}
}

func NewDirectoryBackedClient(ctx context.Context, dirAddr string, tlsConfig *tls.Config, timeout, refreshTTL time.Duration) (*Client, error) {
	coord, err := client.NewDirectoryBackedCoordinator(ctx, dirAddr, tlsConfig, timeout, refreshTTL)
	if err != nil {
		return nil, err
	}
	return &Client{backend: &coordinatorBackend{inner: coord}}, nil
}

func (c *Client) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return c.backend.Read(ctx, hostId, addr, length)
}

func (c *Client) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return c.backend.Write(ctx, hostId, addr, data)
}

// ExecuteAndCommit submits the operations as one minitransaction.
func (c *Client) ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
	return c.backend.ExecuteAndCommit(ctx, class, compares, reads, writes)
}

func (c *Client) GetStats(hostId uint32) (Stats, error) {
	return c.backend.GetStats(hostId)
}

func (c *Client) SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error {
	type retryConfigurable interface {
		SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error
	}
	if cfg, ok := c.backend.(retryConfigurable); ok {
		return cfg.SetRetryPolicy(maxRetries, baseBackoff, maxBackoff)
	}
	return errors.New("retry policy is not supported by this backend")
}

// FlushPendingCommits waits for queued commit/abort messages to reach the nodes.
// ExecuteAndCommit can return once the decision is recoverable, before those
// messages finish. Flush when you need to wait for delivery and check its errors.
// This does nothing if a custom backend doesn't provide a flush operation.
func (c *Client) FlushPendingCommits(ctx context.Context) error {
	type commitFlusher interface {
		FlushPendingCommits(context.Context) error
	}
	if flusher, ok := c.backend.(commitFlusher); ok {
		return flusher.FlushPendingCommits(ctx)
	}
	return nil
}

func txnRateGroupFromInternal(in memnode.TxnRateGroup) TxnRateGroup {
	return TxnRateGroup{
		ExecutedPerSec: RateWindows(in.ExecutedPerSec),
		RetriedPerSec:  RateWindows(in.RetriedPerSec),
		AbortedPerSec:  RateWindows(in.AbortedPerSec),
	}
}

func statsFromInternal(stats memnode.StatsResponse) Stats {
	out := Stats{NodeID: stats.NodeID, Role: stats.Role, ByClass: map[string]TxnClassStats{}}
	out.Txns.Total = stats.Txns.Total
	out.Txns.Committed = stats.Txns.Committed
	out.Txns.Aborted = stats.Txns.Aborted
	out.Txns.Retried = stats.Txns.Retried
	out.Rates.Txns = txnRateGroupFromInternal(stats.Rates.Txns)
	out.Rates.BytesWrittenPerSec = RateWindows(stats.Rates.BytesWrittenPerSec)
	for class, classStats := range stats.ByClass {
		out.ByClass[class] = TxnClassStats{Total: classStats.Total, Committed: classStats.Committed, Aborted: classStats.Aborted, Retried: classStats.Retried, Rates: txnRateGroupFromInternal(classStats.Rates)}
	}
	if len(out.ByClass) == 0 {
		out.ByClass = nil
	}
	return out
}

type coordinatorBackend struct {
	inner interface {
		Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
		Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
		GetStats(hostId uint32) (memnode.StatsResponse, error)
		ExecuteAndCommit(ctx context.Context, class string, compares []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) client.ExecResult
	}
}

func (b *coordinatorBackend) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return b.inner.Read(ctx, hostId, addr, length)
}

func (b *coordinatorBackend) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return b.inner.Write(ctx, hostId, addr, data)
}

func (b *coordinatorBackend) GetStats(hostId uint32) (Stats, error) {
	stats, err := b.inner.GetStats(hostId)
	if err != nil {
		return Stats{}, err
	}
	return statsFromInternal(stats), nil
}

func (b *coordinatorBackend) ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
	internalCmp := make([]proto.CmpItem, 0, len(compares))
	for _, item := range compares {
		internalCmp = append(internalCmp, proto.CmpItem{MemID: item.HostId, Addr: item.Address, Data: append([]byte(nil), item.Data...)})
	}
	internalReads := make([]proto.ReadItem, 0, len(reads))
	for _, item := range reads {
		internalReads = append(internalReads, proto.ReadItem{MemID: item.HostId, Addr: item.Address, Len: item.Length})
	}
	internalWrites := make([]proto.WriteItem, 0, len(writes))
	for _, item := range writes {
		internalWrites = append(internalWrites, proto.WriteItem{MemID: item.HostId, Addr: item.Address, Data: append([]byte(nil), item.Data...)})
	}
	res := b.inner.ExecuteAndCommit(ctx, class, internalCmp, internalReads, internalWrites)
	readsOut := make(map[ReadKey][]byte, len(res.ReadResults))
	for k, v := range res.ReadResults {
		readsOut[ReadKey{HostId: k.MemID, Address: k.Addr, Length: k.Len}] = append([]byte(nil), v...)
	}
	votes := map[uint32]int{}
	for hostId, vote := range res.ParticipantVotes {
		votes[hostId] = int(vote)
	}
	if res.UnknownOutcome {
		return ExecResult{Committed: false, UnknownOutcome: true, ReadResults: map[ReadKey][]byte{}, Status: StatusUnknown, ParticipantVotes: votes}
	}
	if !res.Committed {
		return ExecResult{Committed: false, ReadResults: map[ReadKey][]byte{}, Status: StatusAborted, ParticipantVotes: votes}
	}
	return ExecResult{Committed: true, ReadResults: readsOut, Status: StatusCommitted, ParticipantVotes: votes}
}

func (b *coordinatorBackend) SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error {
	if cfg, ok := b.inner.(interface {
		SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error
	}); ok {
		return cfg.SetRetryPolicy(maxRetries, baseBackoff, maxBackoff)
	}
	return errors.New("retry policy is not supported by backend implementation")
}

func (b *coordinatorBackend) FlushPendingCommits(ctx context.Context) error {
	if flusher, ok := b.inner.(interface {
		FlushPendingCommits(context.Context) error
	}); ok {
		return flusher.FlushPendingCommits(ctx)
	}
	return nil
}

func (c *Client) NewMiniTransaction() *MiniTransaction {
	return minitxn.NewMiniTransaction(c)
}
