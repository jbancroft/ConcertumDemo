package client

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

type DirectoryBackedCoordinator struct {
	mu          sync.Mutex
	dirClient   *DirectoryClient
	tlsConfig   *tls.Config
	timeout     time.Duration
	refreshTTL  time.Duration
	lastRefresh time.Time
	stale       bool
	coord       *Coordinator
	pending     map[*Coordinator]uint64
	pendingSeq  uint64
	retryMax    int
	retryBase   time.Duration
	retryMaxDur time.Duration
}

func NewDirectoryBackedCoordinator(ctx context.Context, dirAddr string, tlsConfig *tls.Config, timeout, refreshTTL time.Duration) (*DirectoryBackedCoordinator, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if refreshTTL <= 0 {
		refreshTTL = 5 * time.Second
	}
	d := &DirectoryBackedCoordinator{
		dirClient:   NewDirectoryClient(dirAddr, tlsConfig),
		tlsConfig:   tlsConfig,
		timeout:     timeout,
		refreshTTL:  refreshTTL,
		retryMax:    5,
		retryBase:   5 * time.Millisecond,
		retryMaxDur: 100 * time.Millisecond,
		pending:     map[*Coordinator]uint64{},
	}
	d.dirClient.SetTimeout(timeout)
	if err := d.refresh(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DirectoryBackedCoordinator) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	coord, err := d.current(ctx)
	if err != nil {
		return nil, err
	}
	data, err := coord.Read(ctx, hostId, addr, length)
	if isRoutingOrTransportError(err) {
		d.markStale()
	}
	return data, err
}

func (d *DirectoryBackedCoordinator) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	coord, err := d.current(ctx)
	if err != nil {
		return err
	}
	err = coord.Write(ctx, hostId, addr, data)
	if isRoutingOrTransportError(err) {
		d.markStale()
	}
	return err
}

func (d *DirectoryBackedCoordinator) GetStats(hostId uint32) (memnode.StatsResponse, error) {
	coord, err := d.current(context.Background())
	if err != nil {
		return memnode.StatsResponse{}, err
	}
	stats, err := coord.GetStats(hostId)
	if isRoutingOrTransportError(err) {
		d.markStale()
	}
	return stats, err
}

func (d *DirectoryBackedCoordinator) ExecuteAndCommit(ctx context.Context, class string, compares []sinfonia.CmpItem, reads []sinfonia.ReadItem, writes []sinfonia.WriteItem) ExecResult {
	coord, err := d.current(ctx)
	if err != nil {
		return ExecResult{Committed: false, ReadResults: map[sinfonia.ReadItem][]byte{}, Status: sinfonia.VoteBadInternal, ParticipantVotes: map[uint32]sinfonia.Vote{}, TransportErr: err}
	}
	res := coord.ExecuteAndCommit(ctx, class, compares, reads, writes)
	d.mu.Lock()
	d.pendingSeq++
	d.pending[coord] = d.pendingSeq
	d.mu.Unlock()
	if res.TransportErr != nil {
		d.markStale()
	}
	return res
}

func (d *DirectoryBackedCoordinator) FlushPendingCommits(ctx context.Context) error {
	d.mu.Lock()
	pending := make(map[*Coordinator]uint64, len(d.pending))
	for coord, sequence := range d.pending {
		pending[coord] = sequence
	}
	d.mu.Unlock()

	var firstErr error
	for coord, sequence := range pending {
		if err := coord.FlushPendingCommits(ctx); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		d.mu.Lock()
		if d.pending[coord] == sequence {
			delete(d.pending, coord)
		}
		d.mu.Unlock()
	}
	return firstErr
}

func (d *DirectoryBackedCoordinator) SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if maxRetries < 0 {
		return errors.New("max retries must be >= 0")
	}
	if baseBackoff <= 0 || maxBackoff <= 0 {
		return errors.New("retry backoff durations must be > 0")
	}
	if baseBackoff > maxBackoff {
		return errors.New("retry backoff base must be <= max")
	}
	d.retryMax = maxRetries
	d.retryBase = baseBackoff
	d.retryMaxDur = maxBackoff
	if d.coord != nil {
		return d.coord.SetRetryPolicy(maxRetries, baseBackoff, maxBackoff)
	}
	return nil
}

func (d *DirectoryBackedCoordinator) current(ctx context.Context) (*Coordinator, error) {
	d.mu.Lock()
	needsRefresh := d.coord == nil || d.stale || time.Since(d.lastRefresh) >= d.refreshTTL
	d.mu.Unlock()
	if needsRefresh {
		if err := d.refresh(ctx); err != nil {
			return nil, err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.coord == nil {
		return nil, ErrUnknownMemNode
	}
	return d.coord, nil
}

func (d *DirectoryBackedCoordinator) refresh(ctx context.Context) error {
	entries, err := d.dirClient.GetDirectory(ctx)
	if err != nil {
		return err
	}
	nodes := map[uint32]Participant{}
	statsReaders := map[uint32]StatsReader{}
	generations := map[uint32]uint64{}
	for hostId, entry := range entries {
		nodes[hostId] = NewRPCParticipantWithGeneration(entry.Primary.RPC, d.tlsConfig, d.timeout, entry.Generation)
		generations[hostId] = entry.Generation
		if entry.Primary.HTTP != "" {
			statsReaders[hostId] = NewAdminHTTPClient(entry.Primary.HTTP, d.timeout)
		}
	}
	coord := NewCoordinator(nodes)
	if err := coord.SetRetryPolicy(d.retryMax, d.retryBase, d.retryMaxDur); err != nil {
		return err
	}
	coord.SetGenerations(generations)
	coord.SetStatsReaders(statsReaders)

	d.mu.Lock()
	defer d.mu.Unlock()
	d.coord = coord
	d.lastRefresh = time.Now()
	d.stale = false
	return nil
}

func (d *DirectoryBackedCoordinator) markStale() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stale = true
}

func isRoutingOrTransportError(err error) bool {
	if err == nil {
		return false
	}
	var remoteErr *rpcwire.RemoteError
	return !errors.As(err, &remoteErr)
}
