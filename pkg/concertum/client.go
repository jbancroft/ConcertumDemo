package concertum

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/jbancroft/concertum2/internal/client"
	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
)

type ReadKey struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

type Status int

const (
	StatusUnknown Status = iota
	StatusCommitted
	StatusAborted
)

type CmpItem struct {
	MemID      uint32
	Addr       uint64
	Data       []byte
	ValidUntil time.Time
}

type ReadItem struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

type WriteItem struct {
	MemID uint32
	Addr  uint64
	Data  []byte
}

type ExecResult struct {
	Committed        bool
	ReadResults      map[ReadKey][]byte
	Status           Status
	ParticipantVotes map[uint32]int
	UnknownOutcome   bool
}

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

// Backend defines the coordinator backend needed by Client.
type Backend interface {
	Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, memID uint32, addr uint64, data []byte) error
	GetStats(memID uint32) (Stats, error)
	ExecAndCommit(ctx context.Context, class string, cmp []CmpItem, reads []ReadItem, writes []WriteItem) ExecResult
}

// Client exposes the public coordinator-backed API.
type Client struct {
	backend Backend
}

func NewClient(backend any) *Client {
	switch b := backend.(type) {
	case Backend:
		return &Client{backend: b}
	case interface {
		Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error)
		Write(ctx context.Context, memID uint32, addr uint64, data []byte) error
		GetStats(memID uint32) (memnode.StatsResponse, error)
		ExecAndCommit(ctx context.Context, class string, cmp []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) client.ExecResult
	}:
		return &Client{backend: &coordinatorBackend{inner: b}}
	default:
		panic("concertum.NewClient: unsupported backend")
	}
}

func NewDirectoryBackedClient(ctx context.Context, dirAddr string, tlsConfig *tls.Config, timeout, refreshTTL time.Duration) (*Client, error) {
	coord, err := client.NewDirectoryBackedCoordinator(ctx, dirAddr, tlsConfig, timeout, refreshTTL)
	if err != nil {
		return nil, err
	}
	return &Client{backend: &coordinatorBackend{inner: coord}}, nil
}

func (c *Client) Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error) {
	return c.backend.Read(ctx, memID, addr, length)
}

func (c *Client) Write(ctx context.Context, memID uint32, addr uint64, data []byte) error {
	return c.backend.Write(ctx, memID, addr, data)
}

func (c *Client) GetStats(memID uint32) (Stats, error) {
	return c.backend.GetStats(memID)
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

// FlushPendingCommits waits for asynchronous phase-two decisions from completed
// ExecAndCommit calls on coordinator-backed clients. The transaction decision is
// already recoverable from participant vote state at return (and logged in
// durable modes); flushing provides prompt visibility and delivery errors.
// Custom backends remain a no-op for compatibility.
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
		Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error)
		Write(ctx context.Context, memID uint32, addr uint64, data []byte) error
		GetStats(memID uint32) (memnode.StatsResponse, error)
		ExecAndCommit(ctx context.Context, class string, cmp []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) client.ExecResult
	}
}

func (b *coordinatorBackend) Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error) {
	return b.inner.Read(ctx, memID, addr, length)
}

func (b *coordinatorBackend) Write(ctx context.Context, memID uint32, addr uint64, data []byte) error {
	return b.inner.Write(ctx, memID, addr, data)
}

func (b *coordinatorBackend) GetStats(memID uint32) (Stats, error) {
	stats, err := b.inner.GetStats(memID)
	if err != nil {
		return Stats{}, err
	}
	return statsFromInternal(stats), nil
}

func (b *coordinatorBackend) ExecAndCommit(ctx context.Context, class string, cmp []CmpItem, reads []ReadItem, writes []WriteItem) ExecResult {
	internalCmp := make([]proto.CmpItem, 0, len(cmp))
	for _, item := range cmp {
		internalCmp = append(internalCmp, proto.CmpItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...), ValidUntil: item.ValidUntil})
	}
	internalReads := make([]proto.ReadItem, 0, len(reads))
	for _, item := range reads {
		internalReads = append(internalReads, proto.ReadItem{MemID: item.MemID, Addr: item.Addr, Len: item.Len})
	}
	internalWrites := make([]proto.WriteItem, 0, len(writes))
	for _, item := range writes {
		internalWrites = append(internalWrites, proto.WriteItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...)})
	}
	res := b.inner.ExecAndCommit(ctx, class, internalCmp, internalReads, internalWrites)
	readsOut := make(map[ReadKey][]byte, len(res.ReadResults))
	for k, v := range res.ReadResults {
		readsOut[ReadKey{MemID: k.MemID, Addr: k.Addr, Len: k.Len}] = append([]byte(nil), v...)
	}
	votes := map[uint32]int{}
	for memID, vote := range res.ParticipantVotes {
		votes[memID] = int(vote)
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

// MTx buffers minitransaction items until ExecAndCommit.
type MTx struct {
	client     *Client
	class      string
	cmpItems   []CmpItem
	readItems  []ReadItem
	writeItems []WriteItem
}

func (c *Client) NewMTx() *MTx {
	return &MTx{client: c}
}

func (m *MTx) Cmp(memID uint32, addr uint64, expected []byte) *MTx {
	m.cmpItems = append(m.cmpItems, CmpItem{MemID: memID, Addr: addr, Data: append([]byte(nil), expected...)})
	return m
}

// CmpValidUntil adds a compare that also aborts unless the transaction reaches
// its commit decision before validUntil. Durable recovery preserves an expiry
// abort selected after all participants prepare successfully.
func (m *MTx) CmpValidUntil(memID uint32, addr uint64, expected []byte, validUntil time.Time) *MTx {
	m.cmpItems = append(m.cmpItems, CmpItem{MemID: memID, Addr: addr, Data: append([]byte(nil), expected...), ValidUntil: validUntil})
	return m
}

// WithClass tags this minitransaction with an optional application class for stats attribution.
func (m *MTx) WithClass(class string) *MTx {
	m.class = class
	return m
}

func (m *MTx) Read(memID uint32, addr uint64, length uint32) *MTx {
	m.readItems = append(m.readItems, ReadItem{MemID: memID, Addr: addr, Len: length})
	return m
}

func (m *MTx) Write(memID uint32, addr uint64, data []byte) *MTx {
	m.writeItems = append(m.writeItems, WriteItem{MemID: memID, Addr: addr, Data: append([]byte(nil), data...)})
	return m
}

func (m *MTx) ExecAndCommit(ctx context.Context) (bool, map[ReadKey][]byte, Status) {
	committed, reads, status, _ := m.ExecAndCommitDetailed(ctx)
	return committed, reads, status
}

func (m *MTx) ExecAndCommitDetailed(ctx context.Context) (bool, map[ReadKey][]byte, Status, map[uint32]int) {
	res := m.client.backend.ExecAndCommit(ctx, m.class, m.cmpItems, m.readItems, m.writeItems)
	return res.Committed, res.ReadResults, res.Status, res.ParticipantVotes
}
