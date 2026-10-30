package memnode_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type gatedRecoveryReplica struct {
	*memnode.Participant
	entered chan struct{}
	release chan struct{}
}

func (p *gatedRecoveryReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	close(p.entered)
	select {
	case <-ctx.Done():
		return sinfonia.ReplicateRedoEntryResponse{}, ctx.Err()
	case <-p.release:
	}
	return p.Participant.ReplicateRedoEntry(ctx, req)
}

func TestRecoveryDoesNotForceAbortAnInFlightRequest(t *testing.T) {
	for _, onePhase := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare", true: "one-phase"}[onePhase], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			primary, err := memnode.NewDurableParticipant(1, 16, filepath.Join(dir, "primary"))
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			replica, err := memnode.NewDurableParticipant(2, 16, filepath.Join(dir, "replica"))
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replica.ConfigureAsReplica(1)
			gate := &gatedRecoveryReplica{Participant: replica, entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(gate.release) })
			defer release()
			primary.ConfigureAsPrimary(2, gate)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			done := make(chan error, 1)
			go func() {
				writes := []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}
				var vote sinfonia.Vote
				var err error
				if onePhase {
					resp, e := primary.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: writes})
					vote, err = resp.Vote, e
				} else {
					resp, e := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: writes})
					vote, err = resp.Vote, e
				}
				if err == nil && vote != sinfonia.VoteOk {
					err = errors.New("request rejected after recovery race")
				}
				done <- err
			}()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
			_, err = primary.RecoveryForceAbort(short, sinfonia.RecoverForceAbortRequest{TID: tid})
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("recovery should wait for the original request: %v", err)
			}
			release()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			vote, err := primary.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
			if err != nil || vote.Vote != sinfonia.VoteOk {
				t.Fatalf("recovery reversed successful request: %+v, %v", vote, err)
			}
			if resp, err := primary.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: true, Generation: vote.Generation}); err != nil || !resp.Ack {
				t.Fatalf("decision: %+v, %v", resp, err)
			}
			got, err := primary.ReadBytesForTest(0, 1)
			if err != nil || string(got) != "A" {
				t.Fatalf("recovered value=%q, %v", got, err)
			}
		})
	}
}

func TestRecoveryAbortRejectsLateOnePhaseRequestOnRAMNode(t *testing.T) {
	node := memnode.NewParticipantWithId(1, 16)
	ctx := context.Background()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
	if err != nil || vote.Vote != sinfonia.VoteBadForced {
		t.Fatalf("force abort: %+v, %v", vote, err)
	}
	response, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}})
	if err != nil || response.Vote != sinfonia.VoteBadForced {
		t.Fatalf("late request reversed abort: %+v, %v", response, err)
	}
	got, _ := node.ReadBytesForTest(0, 1)
	if string(got) != "\x00" {
		t.Fatalf("aborted write applied: %q", got)
	}
}
