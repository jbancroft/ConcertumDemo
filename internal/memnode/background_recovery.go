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
	replayGCInterval          = time.Minute
)

// RunRecovery finishes old prepared transactions and expires saved replies until
// ctx is cancelled.
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
	var nextGC time.Time
	for {
		var tick time.Time
		select {
		case <-ctx.Done():
			return
		case tick = <-ticker.C:
		}
		n.mu.Lock()
		ready := n.recoveryComplete && n.restartRequired == nil
		if ready && !tick.Before(nextGC) && !n.locks.globalWriteLock && !n.backupMode {
			if err := n.gcReplayLocked(); err != nil {
				n.failUncertainLocked("expire transaction replies", err)
				ready = false
			}
			nextGC = tick.Add(replayGCInterval)
		}
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
