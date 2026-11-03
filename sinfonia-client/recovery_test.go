package sinfoniaclient

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestBackgroundRecoveryFinishesInterruptedTransaction(t *testing.T) {
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
				node.SetResponseBoundaryHookForTest(func(string, sinfonia.TransactionID) error { return fault })
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
				node.SetResponseBoundaryHookForTest(nil)
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
				if mode == "durable" {
					if err := node.Close(); err != nil {
						t.Fatal(err)
					}
					node = durableAccessNode(t, dir)
					c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
					defer c.Close()
					if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "B" {
						t.Fatalf("restart reapplied the old write: %q, %v", got, err)
					}
				}
			})
		}
	}
}

type disconnectedAfterCommit struct {
	*memnode.Participant
	calls   atomic.Int32
	failure error
}

func (n *disconnectedAfterCommit) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.calls.Add(1)
	_, err := n.Participant.ExecCommit1PC(ctx, req)
	return sinfonia.ExecCommit1PCResponse{}, errors.Join(n.failure, err)
}

func TestClientReportsUnknownAfterLostCommitReply(t *testing.T) {
	for _, api := range []string{"builder", "direct", "write"} {
		t.Run(api, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node := durableAccessNode(t, dir)
			failure := errors.New("connection lost")
			disconnected := &disconnectedAfterCommit{Participant: node, failure: failure}
			c := &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: disconnected})}
			defer c.Close()
			var err error
			if api == "write" {
				err = c.Write(ctx, 1, 0, []byte("A"))
			} else {
				var result ExecResult
				if api == "builder" {
					result = c.NewMiniTransaction().Read(1, 0, 1).Write(1, 0, []byte("A")).ExecuteAndCommit(ctx)
				} else {
					result = c.ExecuteAndCommit(ctx, nil, []ReadItem{{HostId: 1, Length: 1}}, []WriteItem{{HostId: 1, Data: []byte("A")}})
				}
				if !result.UnknownOutcome || result.Status != StatusUnknown || result.Committed {
					t.Fatalf("expected unknown outcome: %+v", result)
				}
				err = result.OutcomeError()
			}
			if !errors.Is(err, ErrUnknownOutcome) || !errors.Is(err, failure) || disconnected.calls.Load() != 1 {
				t.Fatalf("expected one uncertain attempt preserving its cause: %v, calls=%d", err, disconnected.calls.Load())
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node = durableAccessNode(t, dir)
			c = &Client{coordinator: client.NewCoordinator(map[uint32]client.Participant{1: node})}
			defer c.Close()
			if got, err := c.Read(ctx, 1, 0, 1); err != nil || string(got) != "A" {
				t.Fatalf("unknown result must not undo the committed write: %q, %v", got, err)
			}
		})
	}
}
