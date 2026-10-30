package memnode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestPreparedComparisonProtectsPartiallyWrittenRange(t *testing.T) {
	ctx := context.Background()
	n := NewParticipantWithId(1, 2)
	first := sinfonia.ExecPrepareRequest{
		TID:          lockTestTID(1),
		CmpItems:     []sinfonia.CmpItem{{Addr: 0, Data: []byte{0, 0}}},
		WriteItems:   []sinfonia.WriteItem{{Addr: 0, Data: []byte{1}}},
		Participants: []uint32{1, 2},
	}
	if resp, err := n.ExecAndPrepare(ctx, first); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("first prepare = %+v, %v", resp, err)
	}
	second := first
	second.TID = lockTestTID(2)
	second.WriteItems = []sinfonia.WriteItem{{Addr: 1, Data: []byte{1}}}
	if resp, err := n.ExecAndPrepare(ctx, second); err != nil || resp.Vote != sinfonia.VoteBadLock {
		t.Fatalf("competing prepare = %+v, %v; want lock conflict", resp, err)
	}
	if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: first.TID, ActionCommit: true}); err != nil || !resp.Ack {
		t.Fatalf("first commit = %+v, %v", resp, err)
	}
	second.TID = lockTestTID(3)
	if resp, err := n.ExecAndPrepare(ctx, second); err != nil || resp.Vote != sinfonia.VoteBadCmp {
		t.Fatalf("retry = %+v, %v; want failed comparison after first commit", resp, err)
	}
	if data, err := n.Read(ctx, 0, 2); err != nil || !bytes.Equal(data, []byte{1, 0}) {
		t.Fatalf("final bytes = %v, %v; want [1 0]", data, err)
	}
}

func TestTransactionPartialReadLockConflicts(t *testing.T) {
	for _, onePhase := range []bool{false, true} {
		for _, kind := range []string{"compare", "read"} {
			for _, heldAddr := range []uint64{0, 2} {
				name := kind + "/prepare"
				if onePhase {
					name = kind + "/one-phase"
				}
				if heldAddr == 0 {
					name += "/left"
				} else {
					name += "/right"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					n := NewParticipant(3)
					holder := sinfonia.ExecPrepareRequest{
						TID:        lockTestTID(1),
						WriteItems: []sinfonia.WriteItem{{Addr: heldAddr, Data: []byte{1}}},
					}
					if resp, err := n.ExecAndPrepare(ctx, holder); err != nil || resp.Vote != sinfonia.VoteOk {
						t.Fatalf("holder prepare = %+v, %v", resp, err)
					}
					// The requested read/compare extends to both sides of its own write.
					req := sinfonia.ExecPrepareRequest{
						TID:        lockTestTID(2),
						WriteItems: []sinfonia.WriteItem{{Addr: 1, Data: []byte{1}}},
					}
					if kind == "compare" {
						req.CmpItems = []sinfonia.CmpItem{{Addr: 0, Data: []byte{0, 0, 0}}}
					} else {
						req.ReadItems = []sinfonia.ReadItem{{Addr: 0, Len: 3}}
					}
					vote, _ := runLockTestRequest(t, n, onePhase, req)
					if vote != sinfonia.VoteBadLock {
						t.Fatalf("vote = %v, want lock conflict on the uncovered byte", vote)
					}
					// A failed acquisition must not leave the requested write locked.
					probe := sinfonia.ExecPrepareRequest{
						TID:        lockTestTID(3),
						WriteItems: []sinfonia.WriteItem{{Addr: 1, Data: []byte{0}}},
					}
					if vote, _ := runLockTestRequest(t, n, true, probe); vote != sinfonia.VoteOk {
						t.Fatalf("failed acquisition leaked a lock: probe vote = %v", vote)
					}
					if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: holder.TID, ActionCommit: false}); err != nil || !resp.Ack {
						t.Fatalf("holder abort = %+v, %v", resp, err)
					}
					req.TID = lockTestTID(4)
					if vote, _ := runLockTestRequest(t, n, onePhase, req); vote != sinfonia.VoteOk {
						t.Fatalf("retry after holder aborted = %v, want OK", vote)
					}
				})
			}
		}
	}
}

func TestTransactionOverlappingItemsReleaseLocks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		onePhase bool
		commit   bool
	}{
		{name: "prepare-commit", commit: true},
		{name: "prepare-abort"},
		{name: "one-phase", onePhase: true, commit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			n := NewParticipant(5)
			req := sinfonia.ExecPrepareRequest{
				TID:      lockTestTID(1),
				CmpItems: []sinfonia.CmpItem{{Addr: 0, Data: make([]byte, 5)}},
				ReadItems: []sinfonia.ReadItem{
					{Addr: 0, Len: 5},
					{Addr: 1, Len: 2},
					{Addr: 2, Len: 0},
				},
				WriteItems: []sinfonia.WriteItem{
					{Addr: 1, Data: []byte{1, 2}},
					{Addr: 2, Data: []byte{3, 4}},
				},
			}
			vote, reads := runLockTestRequest(t, n, tc.onePhase, req)
			if vote != sinfonia.VoteOk || !bytes.Equal(reads[0], make([]byte, 5)) || !bytes.Equal(reads[1], []byte{0, 0}) {
				t.Fatalf("overlapping items: vote = %v, reads = %v", vote, reads)
			}
			if !tc.onePhase {
				if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: req.TID, ActionCommit: tc.commit}); err != nil || !resp.Ack {
					t.Fatalf("decision = %+v, %v", resp, err)
				}
			}
			want := make([]byte, 5)
			if tc.commit {
				want = []byte{0, 1, 3, 4, 0}
			}
			if data, err := n.Read(ctx, 0, 5); err != nil || !bytes.Equal(data, want) {
				t.Fatalf("stored bytes = %v, %v; want %v", data, err, want)
			}
			probe := sinfonia.ExecPrepareRequest{
				TID:        lockTestTID(2),
				WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: make([]byte, 5)}},
			}
			if vote, _ := runLockTestRequest(t, n, true, probe); vote != sinfonia.VoteOk {
				t.Fatalf("completed transaction leaked a lock: probe vote = %v", vote)
			}
		})
	}
}

func TestTransactionReadLocksRemainShared(t *testing.T) {
	ctx := context.Background()
	n := NewParticipant(3)
	for counter := uint64(1); counter <= 2; counter++ {
		req := sinfonia.ExecPrepareRequest{
			TID:       lockTestTID(counter),
			CmpItems:  []sinfonia.CmpItem{{Addr: 0, Data: []byte{0, 0, 0}}},
			ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 3}},
		}
		if resp, err := n.ExecAndPrepare(ctx, req); err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("shared reader %d = %+v, %v", counter, resp, err)
		}
	}
	if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: lockTestTID(1), ActionCommit: true}); err != nil || !resp.Ack {
		t.Fatalf("first reader commit = %+v, %v", resp, err)
	}
	writer := sinfonia.ExecPrepareRequest{TID: lockTestTID(3), WriteItems: []sinfonia.WriteItem{{Addr: 1, Data: []byte{1}}}}
	if vote, _ := runLockTestRequest(t, n, true, writer); vote != sinfonia.VoteBadLock {
		t.Fatalf("writer bypassed remaining reader: vote = %v", vote)
	}
	if resp, err := n.Commit(ctx, sinfonia.CommitRequest{TID: lockTestTID(2), ActionCommit: false}); err != nil || !resp.Ack {
		t.Fatalf("second reader abort = %+v, %v", resp, err)
	}
	writer.TID = lockTestTID(4)
	if vote, _ := runLockTestRequest(t, n, true, writer); vote != sinfonia.VoteOk {
		t.Fatalf("reader locks leaked: writer vote = %v", vote)
	}
}

func lockTestTID(counter uint64) sinfonia.TransactionID {
	return sinfonia.TransactionID{CoordinatorID: 1, Counter: counter, Epoch: sinfonia.EpochAt(time.Now())}
}

func runLockTestRequest(t *testing.T, n *Participant, onePhase bool, req sinfonia.ExecPrepareRequest) (sinfonia.Vote, map[uint32][]byte) {
	t.Helper()
	if onePhase {
		resp, err := n.ExecCommit1PC(context.Background(), sinfonia.ExecCommit1PCRequest{
			TID: req.TID, CmpItems: req.CmpItems, ReadItems: req.ReadItems, WriteItems: req.WriteItems,
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Vote, resp.ReadData
	}
	resp, err := n.ExecAndPrepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Vote, resp.ReadData
}
