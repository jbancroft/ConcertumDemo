package client

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Coordinator IDs, transaction projections, retries, and resolution after lost replies.

func TestCoordinatorResolves1PCResponseLostAfterApply(t *testing.T) {
	n := &lost1PCNode{Participant: memnode.NewParticipant(16)}
	coord := NewCoordinator(map[uint32]Participant{1: n})
	defer coord.Close()
	res := coord.ExecuteAndCommit(context.Background(), nil, nil, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}})
	if res.UnknownOutcome || res.TransportErr != nil || !res.Committed {
		t.Fatalf("expected recovered commit, got %+v", res)
	}
	got, err := readParticipant(context.Background(), n.Participant, 0, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("expected applied write despite lost response, got %q err=%v", got, err)
	}
}

func TestCoordinatorResolvesPrepareReplyFailsAfterVote(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
	}{
		{name: "lost reply", failure: errors.New("lost prepare response")},
		{name: "server error", failure: &rpcwire.RemoteError{Method: sinfonia.MethodExecPrepare, Message: "failed to save reply after yes vote"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n1 := &lostPrepareNode{Participant: memnode.NewParticipant(16), failure: tc.failure}
			n2 := memnode.NewParticipant(16)
			coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
			defer coord.Close()
			coord.coordID = 1
			res := coord.ExecuteAndCommit(context.Background(), nil, []sinfonia.ReadItem{{MemID: 1, Len: 1}, {MemID: 2, Len: 1}}, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}, {MemID: 2, Addr: 0, Data: []byte("B")}})
			if res.UnknownOutcome || res.TransportErr != nil || !res.Committed {
				t.Fatalf("expected recovered commit, got %+v", res)
			}
			for id, node := range map[uint32]*memnode.Participant{1: n1.Participant, 2: n2} {
				if got := res.ReadResults[sinfonia.ReadItem{MemID: id, Len: 1}]; string(got) != "\x00" {
					t.Fatalf("node %d lost original read: %q", id, got)
				}
				pending, err := node.QueryInDoubt(context.Background(), sinfonia.QueryInDoubtRequest{})
				if err != nil || len(pending.Entries) != 0 {
					t.Fatalf("node %d kept transaction locked: %+v, %v", id, pending, err)
				}
			}
			resp, err := n1.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}, RequestID: "retry", Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}})
			if err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("expected idempotent prepared vote on retry, resp=%+v err=%v", resp, err)
			}
		})
	}
}

func TestCoordinatorIDsAreDistinctAndNonZero(t *testing.T) {
	first := NewCoordinator(nil)
	defer first.Close()
	second := NewCoordinator(nil)
	defer second.Close()
	if first.coordID == 0 || second.coordID == 0 {
		t.Fatalf("coordinator IDs must be nonzero: first=%d second=%d", first.coordID, second.coordID)
	}
	if first.coordID == second.coordID {
		t.Fatalf("coordinator IDs must be distinct: %d", first.coordID)
	}
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
				recorded := &recordingParticipant{Participant: node}
				nodes := map[uint32]Participant{1: recorded}
				writes := []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}
				if participants == 2 {
					second := memnode.NewParticipant(64)
					defer second.Close()
					nodes[2] = second
					writes = twoParticipantWrites()
				}
				coord := NewCoordinator(nodes)
				defer coord.Close()
				coord.maxRetries = retries
				coord.backoff = retryBackoffFunc(func(ctx context.Context, _ int) {
					// Deliver the failed attempt's abort before testing the epoch retry.
					if err := coord.FlushPendingCommits(ctx); err != nil {
						t.Fatal(err)
					}
				})
				// Even a long-idle coordinator refreshes its epoch before sending.
				coord.epoch = 1
				result := coord.ExecuteAndCommit(ctx, nil, nil, writes)
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
						got, err := coord.Read(ctx, write.MemID, write.Addr, 1)
						if err != nil || string(got) != string(write.Data) {
							t.Fatalf("node %d: got=%q, err=%v", write.MemID, got, err)
						}
					}
				}
			})
		}
	}
}

func TestCoordinatorRetriesSingleParticipantLockConflict(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("release=%t", release), func(t *testing.T) {
			ctx := context.Background()
			node := memnode.NewParticipant(16)
			defer node.Close()
			holder := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID: holder, Participants: []uint32{1},
				WriteItems: []sinfonia.WriteItem{{MemID: 1, Data: []byte("held")}},
			}); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("holder prepare: %+v, %v", resp, err)
			}
			recorded := &recordingParticipant{Participant: node}
			coord := NewCoordinator(map[uint32]Participant{1: recorded})
			defer coord.Close()
			coord.maxRetries = 1
			coord.backoff = retryBackoffFunc(func(context.Context, int) {
				if release {
					if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: holder, ActionCommit: false}); err != nil || !resp.Ack {
						t.Fatalf("holder abort: %+v, %v", resp, err)
					}
				}
			})
			result := coord.ExecuteAndCommit(ctx, nil, nil, []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}})
			want := sinfonia.VoteBadLock
			if release {
				want = sinfonia.VoteOk
			}
			if result.Status != want || result.Committed != release || result.UnknownOutcome || result.TransportErr != nil {
				t.Fatalf("unexpected result: %+v", result)
			}
			if len(recorded.tids) != 2 || recorded.tids[0] == recorded.tids[1] {
				t.Fatalf("expected one retry with a fresh transaction ID: %v", recorded.tids)
			}
		})
	}
}

func TestCoordinatorLogsReadOnlyProjectionWhenTransactionHasWrites(t *testing.T) {
	root := t.TempDir()
	n1, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "n1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n1.Close() })
	n2, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "n2"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n2.Close() })

	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer coord.Close()
	ctx := context.Background()
	result := coord.ExecuteAndCommit(
		ctx,
		nil,
		[]sinfonia.ReadItem{{MemID: 2, Addr: 0, Len: 1}},
		[]sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	)
	if !result.Committed {
		t.Fatalf("transaction should commit: %+v", result)
	}
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}

	if got := n1.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("write participant redo entries=%d want=1", got)
	}
	if got := n2.RedoEntryCountForTest(); got != 1 {
		t.Fatalf("read-only projection redo entries=%d want=1", got)
	}
}

func readParticipant(ctx context.Context, node Participant, addr uint64, length uint32) ([]byte, error) {
	c := NewCoordinator(map[uint32]Participant{1: node})
	defer c.Close()
	c.SetMaxRetriesForTest(0)
	return c.Read(ctx, 1, addr, length)
}

type lost1PCNode struct {
	*memnode.Participant
	dropped bool
}

func (n *lost1PCNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	resp, err := n.Participant.ExecCommit1PC(ctx, req)
	if !n.dropped {
		n.dropped = true
		return sinfonia.ExecCommit1PCResponse{}, errors.New("lost 1pc response")
	}
	return resp, err
}

type lostPrepareNode struct {
	*memnode.Participant
	dropped bool
	failure error
}

func (n *lostPrepareNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	resp, err := n.Participant.ExecAndPrepare(ctx, req)
	if !n.dropped {
		n.dropped = true
		return sinfonia.ExecPrepareResponse{}, n.failure
	}
	return resp, err
}

type recordingParticipant struct {
	Participant
	tids []sinfonia.TransactionID
}

type retryBackoffFunc func(context.Context, int)

func (f retryBackoffFunc) Wait(ctx context.Context, attempt int) { f(ctx, attempt) }

func (n *recordingParticipant) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.tids = append(n.tids, req.TID)
	return n.Participant.ExecAndPrepare(ctx, req)
}

func (n *recordingParticipant) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.tids = append(n.tids, req.TID)
	return n.Participant.ExecCommit1PC(ctx, req)
}

func twoParticipantWrites() []sinfonia.WriteItem {
	return []sinfonia.WriteItem{
		{MemID: 1, Addr: 8, Data: []byte("A")},
		{MemID: 2, Addr: 8, Data: []byte("B")},
	}
}
