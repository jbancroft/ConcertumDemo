package sinfoniaclient

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

func TestBackgroundRecoveryPreservesUnrecordedReads(t *testing.T) {
	for _, phase := range []struct {
		name  string
		onePC bool
	}{{"prepare", false}, {"one-phase", true}} {
		for _, mode := range []string{"RAM", "durable"} {
			t.Run(phase.name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				dir := t.TempDir()
				node := memnode.NewParticipantWithId(1, 16)
				if mode == "durable" {
					node = durableAccessNode(t, dir)
				}
				tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
				fault := errors.New("interrupted before saving original response")
				node.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return fault })
				reads := []sinfonia.ReadItem{{MemID: 1, Len: 1}}
				writes := []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}
				var err error
				if phase.onePC {
					_, err = node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, RequestID: "original", ReadItems: reads, WriteItems: writes})
				} else {
					_, err = node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, RequestID: "original", Participants: []uint32{1}, ReadItems: reads, WriteItems: writes})
				}
				if !errors.Is(err, fault) {
					t.Fatalf("expected interrupted request: %v", err)
				}
				node.SetOutcomeReplayBoundaryHookForTest(nil)
				workerCtx, stopWorker := context.WithCancel(ctx)
				workerDone := make(chan struct{})
				go func() { defer close(workerDone); node.RunRecovery(workerCtx, nil) }()
				defer func() { stopWorker(); <-workerDone }()
				for {
					pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
					if err != nil {
						t.Fatal(err)
					}
					if len(pending.Entries) == 0 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("background recovery did not finish")
					case <-time.After(25 * time.Millisecond):
					}
				}
				stopWorker()
				<-workerDone
				c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
				defer c.Close()
				if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "A" {
					t.Fatalf("background recovery did not apply write: %q, %v", got, err)
				}
				if err := c.Write(ctx, 1, 0, []byte("B")); err != nil {
					t.Fatal(err)
				}
				handle := RecoveryHandle{TID: tid, Participants: []uint32{1}, Reads: []minitxn.RecoveryRead{{ParticipantID: 1, Item: ReadItem{HostId: 1, Length: 1}}}}
				checkReads := func() {
					t.Helper()
					for range 2 {
						result := c.Resolve(ctx, handle)
						if !result.Committed || result.OutcomeError() != nil || string(result.ReadResults[ReadKey{HostId: 1, Length: 1}]) != "\x00" {
							t.Fatalf("recovery discarded original reads: %+v", result)
						}
					}
					if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "B" {
						t.Fatalf("resolution reapplied old write: %q, %v", got, err)
					}
				}
				checkReads()
				if mode == "durable" {
					if err := node.Close(); err != nil {
						t.Fatal(err)
					}
					node = durableAccessNode(t, dir)
					c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
					defer c.Close()
					checkReads()
				}
			})
		}
	}
}

type disconnectedAfterCommit struct {
	*memnode.Participant
	calls atomic.Int32
}

func (n *disconnectedAfterCommit) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.calls.Add(1)
	_, err := n.Participant.ExecCommit1PC(ctx, req)
	return sinfonia.ExecCommit1PCResponse{}, errors.Join(errors.New("connection lost"), err)
}
func (n *disconnectedAfterCommit) RecoveryForceAbort(ctx context.Context, _ sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	<-ctx.Done()
	return sinfonia.RecoverForceAbortResponse{}, ctx.Err()
}

func TestRecoveryHandleSurvivesClientRestartAndPreservesOriginalReads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := durableAccessNode(t, dir)
	disconnected := &disconnectedAfterCommit{Participant: node}
	c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: disconnected})}
	defer c.Close()
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	result := c.NewMiniTransaction().Read(1, 0, 1).Write(1, 0, []byte("A")).ExecuteAndCommit(short)
	cancel()
	if !result.UnknownOutcome || result.Recovery == nil || disconnected.calls.Load() != 1 {
		t.Fatalf("uncertain result must retain original attempt: %+v, calls=%d", result, disconnected.calls.Load())
	}
	saved, err := json.Marshal(result.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node = durableAccessNode(t, dir)
	c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
	defer c.Close()
	// A newer write must neither be replayed over nor substituted for the old read.
	if err := c.Write(ctx, 1, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	var handle RecoveryHandle
	if err := json.Unmarshal(saved, &handle); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		resolved := c.Resolve(ctx, handle)
		if !resolved.Committed || resolved.OutcomeError() != nil || string(resolved.ReadResults[ReadKey{HostId: 1, Length: 1}]) != "\x00" {
			t.Fatalf("resolution lost original result: %+v", resolved)
		}
	}
	if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "B" {
		t.Fatalf("resolution reapplied old write: %q, %v", got, err)
	}
}

func TestWriteUnknownErrorCarriesRecoveryHandle(t *testing.T) {
	node := memnode.NewParticipantWithId(1, 16)
	c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: &disconnectedAfterCommit{Participant: node}})}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := c.Write(ctx, 1, 0, []byte("A"))
	var uncertain *UnknownOutcomeError
	if !errors.Is(err, ErrUnknownOutcome) || !errors.As(err, &uncertain) || uncertain.Handle == nil {
		t.Fatalf("missing recoverable error: %v", err)
	}
	c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
	defer c.Close()
	if resolved := c.Resolve(context.Background(), *uncertain.Handle); !resolved.Committed {
		t.Fatalf("resolution failed: %+v", resolved)
	}
}

func TestResolveExpiredEvidenceDoesNotInventAbort(t *testing.T) {
	c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: memnode.NewParticipantWithId(1, 16)})}
	defer c.Close()
	handle := RecoveryHandle{TID: minitxn.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now()) - 3}, Participants: []uint32{1}}
	result := c.Resolve(context.Background(), handle)
	if !result.UnknownOutcome || result.Status != StatusUnknown || !errors.Is(result.OutcomeError(), ErrOutcomeExpired) {
		t.Fatalf("expired evidence became a decision: %+v", result)
	}
}

func TestResolveCommitWithoutOriginalReadsDoesNotReread(t *testing.T) {
	dir := t.TempDir()
	node := durableAccessNode(t, dir)
	node.SetOutcomeReplayBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return errors.New("lost before saving response") })
	c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: &disconnectedAfterCommit{Participant: node}})}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	result := c.NewMiniTransaction().Read(1, 0, 1).Write(1, 0, []byte("A")).ExecuteAndCommit(ctx)
	cancel()
	if result.Recovery == nil || !result.UnknownOutcome {
		t.Fatalf("expected uncertain attempt: %+v", result)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node = durableAccessNode(t, dir)
	c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
	defer c.Close()
	result = c.Resolve(context.Background(), *result.Recovery)
	if !result.Committed || result.UnknownOutcome || !errors.Is(result.OutcomeError(), ErrReadResultsUnavailable) || len(result.ReadResults) != 0 {
		t.Fatalf("must distinguish known commit from missing read payload: %+v", result)
	}
}
