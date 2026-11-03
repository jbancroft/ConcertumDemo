// Package recovery implements Sinfonia's recovery coordinator: keep existing
// votes, force an abort vote at participants that have not voted, then decide.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const defaultProbeTimeout = 500 * time.Millisecond

type Participant interface {
	RecoveryForceAbort(context.Context, sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error)
	RecoveryDecide(context.Context, sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error)
}

type Result struct {
	Known     bool
	Committed bool
	Err       error
}

type voteResult struct {
	id   uint32
	vote sinfonia.RecoverForceAbortResponse
	err  error
}

// Discover collects votes and chooses an outcome without delivering decisions.
// Startup uses it before applying local redo in log order. A known outcome is
// usable even when Err reports an unavailable peer; missing votes are not aborts.
func Discover(ctx context.Context, tid sinfonia.TransactionID, ids []uint32, peers map[uint32]Participant) Result {
	var calls sync.WaitGroup
	defer calls.Wait()
	out, _ := discover(ctx, tid, ids, peers, &calls)
	return out
}

func discover(ctx context.Context, tid sinfonia.TransactionID, ids []uint32, peers map[uint32]Participant, calls *sync.WaitGroup) (Result, map[uint32]sinfonia.RecoverForceAbortResponse) {
	out := Result{}
	votes := make(map[uint32]sinfonia.RecoverForceAbortResponse)
	if err := ctx.Err(); err != nil {
		return Result{Err: err}, votes
	}
	if len(ids) == 0 {
		out.Err = errors.New("recovery has no participants")
		return out, votes
	}
	// The caller joins probes before returning. Resolve can deliver a known
	// decision while a timed-out probe is still finishing.
	// Reserve time for decision delivery even when one probe is unreachable.
	probeTimeout := defaultProbeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		probeTimeout = min(probeTimeout, time.Until(deadline)/2)
	}
	probeCtx, stopProbes := context.WithTimeout(ctx, probeTimeout)
	defer stopProbes()
	replies := make(chan voteResult, len(ids))
	for _, id := range ids {
		calls.Add(1)
		go func(id uint32) {
			defer calls.Done()
			peer := peers[id]
			if peer == nil {
				replies <- voteResult{id: id, err: fmt.Errorf("recovery participant %d unavailable", id)}
				return
			}
			vote, err := peer.RecoveryForceAbort(probeCtx, sinfonia.RecoverForceAbortRequest{TID: tid, Epoch: tid.Epoch})
			replies <- voteResult{id: id, vote: vote, err: err}
		}(id)
	}
collect:
	for range ids {
		select {
		case reply := <-replies:
			if reply.err != nil {
				out.Err = errors.Join(out.Err, reply.err)
			} else {
				votes[reply.id] = reply.vote
			}
		case <-probeCtx.Done():
			out.Err = errors.Join(out.Err, probeCtx.Err())
			break collect
		}
	}
	allYes, negative := len(votes) == len(ids), false
	for _, vote := range votes {
		if vote.KnownDecision {
			if out.Known && out.Committed != vote.DecisionCommit {
				return Result{Err: errors.New("conflicting transaction decisions")}, votes
			}
			out.Known, out.Committed = true, vote.DecisionCommit
		}
		if vote.Vote != sinfonia.VoteOk {
			allYes = false
			if vote.Vote != sinfonia.VoteUnknown {
				negative = true
			}
		}
	}
	if !out.Known && (allYes || negative) {
		out.Known, out.Committed = true, allYes
	}
	return out, votes
}

// Resolve discovers an outcome and delivers it to reachable participants.
// A known decision remains known even if delivery fails; another recovery pass
// can deliver it later. All probe and delivery calls finish before returning.
func Resolve(ctx context.Context, tid sinfonia.TransactionID, ids []uint32, peers map[uint32]Participant) Result {
	var calls sync.WaitGroup
	defer calls.Wait()
	out, votes := discover(ctx, tid, ids, peers, &calls)
	if !out.Known {
		return out
	}
	deliveries := make(chan error, len(votes))
	pending := 0
	for id := range votes {
		pending++
		calls.Add(1)
		go func(id uint32) {
			defer calls.Done()
			resp, err := peers[id].RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: out.Committed})
			if err == nil && !resp.Ack {
				err = fmt.Errorf("participant %d did not acknowledge recovery", id)
			}
			deliveries <- err
		}(id)
	}
	for range pending {
		select {
		case err := <-deliveries:
			out.Err = errors.Join(out.Err, err)
		case <-ctx.Done():
			out.Err = errors.Join(out.Err, ctx.Err())
			return out
		}
	}
	return out
}
