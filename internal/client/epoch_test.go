package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

type epochRecordingNode struct {
	Participant
	tids []sinfonia.TransactionID
}

type epochRetryBackoff func(context.Context, int)

func (f epochRetryBackoff) Wait(ctx context.Context, attempt int) { f(ctx, attempt) }

func (n *epochRecordingNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.tids = append(n.tids, req.TID)
	return n.Participant.ExecAndPrepare(ctx, req)
}

func (n *epochRecordingNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.tids = append(n.tids, req.TID)
	return n.Participant.ExecCommit1PC(ctx, req)
}

func TestCoordinatorRetriesStaleEpochWithFreshID(t *testing.T) {
	for _, participants := range []int{1, 2} {
		for _, retries := range []int{0, 1} {
			t.Run(fmt.Sprintf("participants=%d/retries=%d", participants, retries), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				dir := t.TempDir()
				current := sinfonia.EpochAt(time.Now())
				ahead := current + 3
				if err := storage.StoreEpochFloor(dir, ahead); err != nil {
					t.Fatal(err)
				}
				node, err := memnode.NewDurableParticipant(1, 64, dir)
				if err != nil {
					t.Fatal(err)
				}
				defer node.Close()
				if err := node.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				recorded := &epochRecordingNode{Participant: node}
				nodes := map[uint32]Participant{1: recorded}
				writes := []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}
				if participants == 2 {
					second := memnode.NewParticipant(64)
					defer second.Close()
					nodes[2] = second
					writes = phaseTwoWrites()
				}
				coord := NewCoordinator(nodes)
				coord.maxRetries = retries
				coord.backoff = epochRetryBackoff(func(ctx context.Context, _ int) {
					// Deliver the failed attempt's abort before testing the epoch retry.
					if err := coord.FlushPendingCommits(ctx); err != nil {
						t.Fatal(err)
					}
				})
				// Even a long-idle coordinator refreshes its epoch before sending.
				coord.epoch = 1
				result := coord.ExecuteAndCommit(ctx, "", nil, nil, writes)
				if result.UnknownOutcome || result.Committed != (retries == 1) {
					t.Fatalf("result=%+v", result)
				}
				if retries == 0 && result.Status != sinfonia.VoteBadStaleEpoch {
					t.Fatalf("expected explicit stale rejection: %+v", result)
				}
				if err := coord.FlushPendingCommits(ctx); err != nil {
					t.Fatal(err)
				}
				if len(recorded.tids) != retries+1 {
					t.Fatalf("attempts=%d, want %d", len(recorded.tids), retries+1)
				}
				first := recorded.tids[0]
				if first.Epoch < current || first.Epoch > current+1 {
					t.Fatalf("first attempt did not use current UTC hour: %+v", first)
				}
				if retries == 1 {
					second := recorded.tids[1]
					if second.Epoch != ahead || second.Counter <= first.Counter || second.CoordinatorID != first.CoordinatorID {
						t.Fatalf("retry did not advance ID and epoch: first=%+v, second=%+v", first, second)
					}
					for _, write := range writes {
						got, err := nodes[write.MemID].Read(ctx, write.Addr, 1)
						if err != nil || string(got) != string(write.Data) {
							t.Fatalf("node %d: got=%q, err=%v", write.MemID, got, err)
						}
					}
				}
			})
		}
	}
}
