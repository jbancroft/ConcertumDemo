package client

import (
	"context"
	"errors"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

const (
	defaultExecuteTimeout    = 5 * time.Second
	resolutionAttemptTimeout = 250 * time.Millisecond
	resolutionRetryDelay     = 25 * time.Millisecond
	resolutionTimeout        = time.Second
)

// Resolve checks the original attempt; it never executes the mutation again.
// Each call is bounded. The returned handle allows another call after an outage.
func (c *Coordinator) Resolve(ctx context.Context, handle minitxn.RecoveryHandle) ExecResult {
	ctx, cancel := context.WithTimeout(ctx, resolutionTimeout)
	defer cancel()
	peers := make(map[uint32]recovery.Participant, len(c.nodes))
	for id, node := range c.nodes {
		peers[id] = node
	}
	unknown := ExecResult{UnknownOutcome: true, Status: sinfonia.VoteUnknown, Recovery: &handle}
	if len(handle.Participants) == 0 {
		unknown.TransportErr = errors.New("recovery handle has no participants")
		return unknown
	}
	for {
		if err := ctx.Err(); err != nil {
			unknown.TransportErr = errors.Join(unknown.TransportErr, err)
			return unknown
		}
		attemptCtx, attemptCancel := context.WithTimeout(ctx, resolutionAttemptTimeout)
		outcome := recovery.Resolve(attemptCtx, handle.TID, handle.Participants, peers)
		attemptCancel()
		if outcome.Known {
			result := ExecResult{Committed: outcome.Committed, Status: sinfonia.VoteBadForced, Recovery: &handle, ParticipantVotes: make(map[uint32]sinfonia.Vote), ReadResults: make(map[sinfonia.ReadItem][]byte)}
			for id, vote := range outcome.Votes {
				result.ParticipantVotes[id] = vote.Vote
			}
			if !outcome.Committed {
				return result
			}
			result.Status = sinfonia.VoteOk
			offsets := make(map[uint32]uint32)
			for _, read := range handle.Reads {
				index := offsets[read.ParticipantID]
				offsets[read.ParticipantID]++
				data, ok := outcome.Votes[read.ParticipantID].ReadData[index]
				if !ok || len(data) != int(read.Item.Length) {
					result.TransportErr = minitxn.ErrReadResultsUnavailable
					continue
				}
				item := sinfonia.ReadItem{MemID: read.Item.HostId, Addr: read.Item.Address, Len: read.Item.Length}
				result.ReadResults[item] = append([]byte(nil), data...)
			}
			return result
		}
		unknown.TransportErr = outcome.Err
		if errors.Is(outcome.Err, minitxn.ErrOutcomeExpired) {
			return unknown
		}
		select {
		case <-ctx.Done():
		case <-time.After(resolutionRetryDelay):
		}
	}
}
