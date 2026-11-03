package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Coordinator IDs, transaction projections, retries, and uncertainty after lost replies.

func newTestParticipant(t testing.TB, id uint32, size int) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewParticipant(id, size, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := node.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	return node
}

func TestCoordinatorRejectsUnknownHostBeforePreparing(t *testing.T) {
	for _, kind := range []string{"compare", "read", "write"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			node := newTestParticipant(t, 1, 16)
			coord := NewCoordinator(map[uint32]Participant{1: node})
			defer coord.Close()
			var compares []sinfonia.CmpItem
			var reads []sinfonia.ReadItem
			writes := []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}
			switch kind {
			case "compare":
				compares = []sinfonia.CmpItem{{MemID: 99, Data: []byte{0}}}
			case "read":
				reads = []sinfonia.ReadItem{{MemID: 99, Len: 1}}
			case "write":
				writes = append(writes, sinfonia.WriteItem{MemID: 99, Data: []byte("B")})
			}
			res := coord.ExecuteAndCommit(ctx, compares, reads, writes)
			if res.Committed || res.UnknownOutcome || !errors.Is(res.TransportErr, ErrUnknownMemNode) {
				t.Fatalf("unknown host must reject the whole transaction: %+v", res)
			}
			pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
			if err != nil || len(pending.Entries) != 0 {
				t.Fatalf("invalid transaction left prepared work: %+v, %v", pending, err)
			}
			got, err := coord.Read(ctx, 1, 0, 1)
			if err != nil || len(got) != 1 || got[0] != 0 {
				t.Fatalf("rejected transaction changed or locked the valid node: %x, %v", got, err)
			}
		})
	}
}

func TestCoordinatorReportsUnknownAfter1PCResponseLost(t *testing.T) {
	failure := errors.New("lost 1pc response")
	n := &lost1PCNode{Participant: newTestParticipant(t, 1, 16), failure: failure}
	coord := NewCoordinator(map[uint32]Participant{1: n})
	defer coord.Close()
	res := coord.ExecuteAndCommit(t.Context(), nil, nil, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}})
	if !res.UnknownOutcome || !errors.Is(res.TransportErr, failure) || res.Committed || n.calls != 1 {
		t.Fatalf("expected one uncertain attempt, got %+v, calls=%d", res, n.calls)
	}
	got, err := readParticipant(t.Context(), n.Participant, 0, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("expected applied write despite lost response, got %q err=%v", got, err)
	}
}

func TestCoordinatorReportsUnknownAfterPrepareReplyFails(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
	}{
		{name: "lost reply", failure: errors.New("lost prepare response")},
		{name: "server error", failure: &rpcwire.RemoteError{Method: sinfonia.MethodExecPrepare, Message: "failed to save reply after yes vote"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n1 := &lostPrepareNode{Participant: newTestParticipant(t, 1, 16), failure: tc.failure}
			n2 := newTestParticipant(t, 2, 16)
			coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
			defer coord.Close()
			coord.coordID = 1
			res := coord.ExecuteAndCommit(t.Context(), nil, []sinfonia.ReadItem{{MemID: 1, Len: 1}, {MemID: 2, Len: 1}}, []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}, {MemID: 2, Addr: 0, Data: []byte("B")}})
			if !res.UnknownOutcome || !errors.Is(res.TransportErr, tc.failure) || res.Committed || n1.calls != 1 {
				t.Fatalf("expected one uncertain attempt, got %+v, calls=%d", res, n1.calls)
			}
			for id, node := range map[uint32]*memnode.Participant{1: n1.Participant, 2: n2} {
				pending, err := node.QueryInDoubt(t.Context(), sinfonia.QueryInDoubtRequest{})
				if err != nil || len(pending.Entries) != 1 {
					t.Fatalf("node %d should await node recovery: %+v, %v", id, pending, err)
				}
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
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				dir := t.TempDir()
				current := sinfonia.EpochAt(time.Now())
				ahead := current + 3
				if err := storage.StoreEpochFloor(dir, ahead); err != nil {
					t.Fatal(err)
				}
				node, err := memnode.NewParticipant(1, 64, dir)
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
					second := newTestParticipant(t, 2, 64)
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
			ctx := t.Context()
			node := newTestParticipant(t, 1, 16)
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

func TestCoordinatorDoesNotRetryComparisonFailureWithLockConflict(t *testing.T) {
	ctx := t.Context()
	n1, n2 := newTestParticipant(t, 1, 16), newTestParticipant(t, 2, 16)
	defer n1.Close()
	defer n2.Close()
	holder := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := n2.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: holder, Participants: []uint32{2},
		WriteItems: []sinfonia.WriteItem{{MemID: 2, Data: []byte("held")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("holder prepare: %+v, %v", resp, err)
	}
	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer coord.Close()
	coord.maxRetries = 1
	retried := false
	coord.backoff = retryBackoffFunc(func(ctx context.Context, _ int) {
		retried = true
		if err := coord.FlushPendingCommits(ctx); err != nil {
			t.Fatal(err)
		}
		if resp, err := n2.Commit(ctx, sinfonia.CommitRequest{TID: holder}); err != nil || !resp.Ack {
			t.Fatalf("holder abort: %+v, %v", resp, err)
		}
		// Both rejection causes disappear, so an incorrect retry would commit.
		if err := coord.Write(ctx, 1, 0, []byte("A")); err != nil {
			t.Fatal(err)
		}
	})
	result := coord.ExecuteAndCommit(ctx,
		[]sinfonia.CmpItem{{MemID: 1, Data: []byte("A")}}, nil,
		[]sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("X")}, {MemID: 2, Data: []byte("Y")}},
	)
	if err := coord.FlushPendingCommits(ctx); err != nil {
		t.Fatal(err)
	}
	if retried || result.Committed || result.UnknownOutcome || result.TransportErr != nil || result.Status != sinfonia.VoteBadCmp {
		t.Fatalf("comparison failure must abort without retry: retried=%t, result=%+v", retried, result)
	}
	if result.ParticipantVotes[1] != sinfonia.VoteBadCmp || result.ParticipantVotes[2] != sinfonia.VoteBadLock {
		t.Fatalf("expected mixed comparison and lock rejections: %v", result.ParticipantVotes)
	}
	if resp, err := n2.Commit(ctx, sinfonia.CommitRequest{TID: holder}); err != nil || !resp.Ack {
		t.Fatalf("holder abort: %+v, %v", resp, err)
	}
	for _, item := range []sinfonia.ReadItem{{MemID: 1, Addr: 1, Len: 1}, {MemID: 2, Len: 1}} {
		if got, err := coord.Read(ctx, item.MemID, item.Addr, item.Len); err != nil || len(got) != 1 || got[0] != 0 {
			t.Fatalf("aborted write on node %d: got=%v, err=%v", item.MemID, got, err)
		}
	}
}

func TestCoordinatorLogsReadOnlyProjectionWhenTransactionHasWrites(t *testing.T) {
	n1, n2 := newTestParticipant(t, 1, 64), newTestParticipant(t, 2, 64)

	coord := NewCoordinator(map[uint32]Participant{1: n1, 2: n2})
	defer coord.Close()
	ctx := t.Context()
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
	calls   int
	failure error
}

func (n *lost1PCNode) ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error) {
	n.calls++
	resp, err := n.Participant.ExecCommit1PC(ctx, req)
	if n.calls == 1 {
		return sinfonia.ExecCommit1PCResponse{}, n.failure
	}
	return resp, err
}

type lostPrepareNode struct {
	*memnode.Participant
	calls   int
	failure error
}

func (n *lostPrepareNode) ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error) {
	n.calls++
	resp, err := n.Participant.ExecAndPrepare(ctx, req)
	if n.calls == 1 {
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
