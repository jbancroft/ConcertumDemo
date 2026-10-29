package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

var (
	ErrDirectoryUpdateOutcomeUnknown = errors.New("directory update outcome unknown")
	ErrDirectoryUpdateConflict       = errors.New("directory update conflict")
	ErrDirectoryStoreDegraded        = errors.New("directory store degraded")
)

type DirectoryClient struct {
	addr    string
	tls     *tls.Config
	timeout time.Duration
	rpc     *rpcwire.Client
}

func NewDirectoryClient(addr string, tlsConfig *tls.Config) *DirectoryClient {
	c := &DirectoryClient{addr: strings.TrimSpace(addr), tls: tlsConfig, timeout: 3 * time.Second}
	c.reset()
	return c
}

func (c *DirectoryClient) SetTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	c.timeout = timeout
	c.reset()
}

func (c *DirectoryClient) GetDirectory(ctx context.Context) (map[uint32]sinfonia.DirectoryEntry, error) {
	var out sinfonia.DirectoryResponse
	if err := c.rpc.Call(ctx, sinfonia.MethodDirectory, sinfonia.DirectoryRequest{}, &out); err != nil {
		return nil, err
	}
	return out.Mappings, nil
}

func (c *DirectoryClient) Resolve(ctx context.Context, hostId uint32) (sinfonia.ResolveResponse, error) {
	var out sinfonia.ResolveResponse
	return out, c.rpc.Call(ctx, sinfonia.MethodResolve, sinfonia.ResolveRequest{MemID: hostId}, &out)
}

func (c *DirectoryClient) UpdateEntry(ctx context.Context, hostId uint32, entry sinfonia.DirectoryEntry) error {
	return c.UpdateEntryCAS(ctx, hostId, entry, 0)
}

func (c *DirectoryClient) UpdateEntryCAS(ctx context.Context, hostId uint32, entry sinfonia.DirectoryEntry, expectedGeneration uint64) error {
	return c.updateEntryCAS(ctx, hostId, entry, expectedGeneration, 0, false)
}

// UpdateEntryCASRevision only updates the entry if the directory revision still
// matches, including revision zero for a new directory.
func (c *DirectoryClient) UpdateEntryCASRevision(ctx context.Context, hostId uint32, entry sinfonia.DirectoryEntry, expectedGeneration, expectedRevision uint64) error {
	return c.updateEntryCAS(ctx, hostId, entry, expectedGeneration, expectedRevision, true)
}

func (c *DirectoryClient) updateEntryCAS(ctx context.Context, hostId uint32, entry sinfonia.DirectoryEntry, expectedGeneration, expectedRevision uint64, expectedRevisionSet bool) error {
	var out sinfonia.UpdateEntryResponse
	if err := c.rpc.Call(ctx, sinfonia.MethodUpdateEntry, sinfonia.UpdateEntryRequest{MemID: hostId, Entry: entry, ExpectedGeneration: expectedGeneration, ExpectedRevision: expectedRevision, ExpectedRevisionSet: expectedRevisionSet}, &out); err != nil {
		var remote *rpcwire.RemoteError
		if errors.As(err, &remote) {
			return err
		}
		return c.reconcileTransportAmbiguity(ctx, hostId, entry, expectedRevision, expectedRevisionSet, err)
	}
	if out.OutcomeUnknown {
		resolved, err := c.Resolve(ctx, hostId)
		if err != nil {
			return fmt.Errorf("%w at revision %d: reconcile: %v", ErrDirectoryUpdateOutcomeUnknown, out.Revision, err)
		}
		if sameDirectoryEntry(entry, resolved) {
			return nil
		}
		if out.Revision == 0 || resolved.DirectoryRevision < out.Revision {
			return fmt.Errorf("%w at revision %d", ErrDirectoryUpdateOutcomeUnknown, out.Revision)
		}
		return fmt.Errorf("%w: requested revision %d, selected revision %d", ErrDirectoryUpdateConflict, out.Revision, resolved.DirectoryRevision)
	}
	if out.Degraded {
		return fmt.Errorf("%w at revision %d", ErrDirectoryStoreDegraded, out.Revision)
	}
	if !out.Ack {
		return &rpcwire.RemoteError{Method: sinfonia.MethodUpdateEntry, Message: "directory update not acknowledged"}
	}
	return nil
}

func (c *DirectoryClient) reconcileTransportAmbiguity(ctx context.Context, hostId uint32, entry sinfonia.DirectoryEntry, expectedRevision uint64, expectedRevisionSet bool, transportErr error) error {
	reconcileCtx := ctx
	var timeoutErr net.Error
	if ctx.Err() != nil || (errors.As(transportErr, &timeoutErr) && timeoutErr.Timeout()) {
		var cancel context.CancelFunc
		reconcileCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
		defer cancel()
	}
	resolved, err := c.Resolve(reconcileCtx, hostId)
	if err != nil {
		return fmt.Errorf("%w: update transport: %v; reconcile: %v", ErrDirectoryUpdateOutcomeUnknown, transportErr, err)
	}
	if sameDirectoryEntry(entry, resolved) {
		return nil
	}
	if !expectedRevisionSet || resolved.DirectoryRevision <= expectedRevision {
		return fmt.Errorf("%w: update transport: %v; selected revision %d", ErrDirectoryUpdateOutcomeUnknown, transportErr, resolved.DirectoryRevision)
	}
	return fmt.Errorf("%w: expected base revision %d, selected revision %d", ErrDirectoryUpdateConflict, expectedRevision, resolved.DirectoryRevision)
}

func sameDirectoryEntry(want sinfonia.DirectoryEntry, got sinfonia.ResolveResponse) bool {
	if want.Generation != got.Generation || want.Primary != got.Primary {
		return false
	}
	if want.Replica == nil || got.Replica == nil {
		return want.Replica == nil && got.Replica == nil
	}
	return *want.Replica == *got.Replica
}

func (c *DirectoryClient) reset() {
	c.rpc = rpcwire.NewClient(c.addr, rpcwire.ClientOptions{TLS: c.tls, DialTimeout: c.timeout})
}
