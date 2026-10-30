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
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

const defaultProbeTimeout = 500 * time.Millisecond

type Participant interface {
	RecoveryForceAbort(context.Context, sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error)
	RecoveryDecide(context.Context, sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error)
}

type Result struct {
	Known     bool
	Committed bool
	Votes     map[uint32]sinfonia.RecoverForceAbortResponse
	Err       error
}

type voteResult struct {
	id   uint32
	vote sinfonia.RecoverForceAbortResponse
	err  error
}

// Resolve makes one pass. A known decision remains known even if delivery fails;
// another recovery pass can deliver it later. Unreachable or expired evidence is
// never treated as an abort vote. Calls run concurrently so an unavailable node
// cannot prevent us from obtaining evidence or delivering decisions elsewhere.
func Resolve(ctx context.Context, tid sinfonia.TransactionID, ids []uint32, peers map[uint32]Participant) Result {
	out := Result{Votes: make(map[uint32]sinfonia.RecoverForceAbortResponse)}
	if len(ids) == 0 {
		out.Err = errors.New("recovery handle has no participants")
		return out
	}
	// Join calls before returning so a shutdown can safely close its local node.
	var calls sync.WaitGroup
	defer calls.Wait()
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
				out.Votes[reply.id] = reply.vote
			}
		case <-probeCtx.Done():
			out.Err = errors.Join(out.Err, probeCtx.Err())
			break collect
		}
	}
	allYes, negative := len(out.Votes) == len(ids), false
	for _, vote := range out.Votes {
		if vote.Expired {
			allYes = false
			out.Err = errors.Join(out.Err, minitxn.ErrOutcomeExpired)
			continue
		}
		if vote.KnownDecision {
			if out.Known && out.Committed != vote.DecisionCommit {
				return Result{Err: errors.New("conflicting transaction decisions"), Votes: out.Votes}
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
	if !out.Known {
		return out
	}
	deliveries := make(chan error, len(out.Votes))
	pending := 0
	for id, vote := range out.Votes {
		if vote.Expired {
			continue
		}
		pending++
		calls.Add(1)
		go func(id uint32, generation uint64) {
			defer calls.Done()
			resp, err := peers[id].RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: out.Committed, Generation: generation})
			if err == nil && !resp.Ack {
				err = fmt.Errorf("participant %d did not acknowledge recovery", id)
			}
			deliveries <- err
		}(id, vote.Generation)
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
