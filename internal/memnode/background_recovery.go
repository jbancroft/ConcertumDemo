package memnode

import (
	"context"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
)

const (
	abandonedTransactionAge   = time.Second
	backgroundRecoveryTimeout = time.Second
	recoveryScanInterval      = 250 * time.Millisecond
)

// RunRecovery helps finish old prepared transactions until ctx is cancelled.
// Concurrent recovery coordinators are safe: participants never reverse votes.
// Call after startup recovery, and wait for it to stop before closing the node.
func (n *Participant) RunRecovery(ctx context.Context, remote map[uint32]recovery.Participant) {
	peers := make(map[uint32]recovery.Participant, len(remote)+1)
	for id, peer := range remote {
		peers[id] = peer
	}
	peers[n.hostId] = n
	ticker := time.NewTicker(recoveryScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		ready := n.recoveryComplete && n.mode != ModeReplReplica
		now := n.nowFn()
		n.mu.Unlock()
		if !ready {
			continue
		}
		pending, _ := n.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
		for _, entry := range pending.Entries {
			if now.Sub(time.Unix(0, entry.PreparedAt)) < abandonedTransactionAge {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			attempt, cancel := context.WithTimeout(ctx, backgroundRecoveryTimeout)
			recovery.Resolve(attempt, entry.TID, entry.Participants, peers)
			cancel()
		}
	}
}
