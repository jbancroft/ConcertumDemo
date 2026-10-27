package memnode

import (
	"context"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/proto"
)

func TestExecCommit1PCRejectsExpiredCompareDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	n := NewParticipant(8)
	n.SetEpochSourceForTest(time.Hour, func() time.Time { return now })
	if err := n.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Minute)
	now = deadline
	resp, err := n.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{
		TID:                             proto.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: 1},
		RequestID:                       "expired-compare",
		CmpItems:                        []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("A"), ValidUntil: deadline}},
		WriteItems:                      []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("B")}},
		Epoch:                           1,
		Generation:                      0,
		DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
	})
	if err != nil || resp.Vote != proto.VoteBadCmp {
		t.Fatalf("expired compare resp=%+v err=%v", resp, err)
	}
	got, err := n.Read(ctx, 0, 1)
	if err != nil || string(got) != "A" {
		t.Fatalf("expired compare applied write: got=%q err=%v", got, err)
	}
}

func TestExecCommit1PCDuplicateTIDReturnsOriginalOutcomeWithoutReapply(t *testing.T) {
	n := NewParticipant(8)
	txId := proto.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: 1}
	req := proto.ExecCommit1PCRequest{TID: txId, RequestID: "first", ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(context.Background(), req)
	if err != nil || resp.Vote != proto.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("first 1pc resp=%+v err=%v", resp, err)
	}
	retry := req
	retry.RequestID = "retry"
	retry.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecCommit1PC(context.Background(), retry)
	if err != nil || resp.Vote != proto.VoteOk || resp.RequestID != "retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("retry 1pc should return original outcome, resp=%+v err=%v", resp, err)
	}
	got, _ := n.Read(context.Background(), 0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate 1pc reapplied write: got %q", got)
	}
}

func TestPrepareAndCommitDuplicateTIDReturnOriginalOutcomeAfterResponseLoss(t *testing.T) {
	n := NewParticipant(8)
	txId := proto.TransactionID{CoordinatorID: 7, Counter: 2, Epoch: 1}
	prep := proto.ExecPrepareRequest{TID: txId, RequestID: "prepare-1", Participants: []uint32{1}, ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != proto.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	prep.RequestID = "prepare-retry"
	prep.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != proto.VoteOk || resp.RequestID != "prepare-retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare retry resp=%+v err=%v", resp, err)
	}
	commit := proto.CommitRequest{TID: txId, RequestID: "commit-1", ActionCommit: true}
	cresp, err := n.Commit(context.Background(), commit)
	if err != nil || !cresp.Ack {
		t.Fatalf("commit resp=%+v err=%v", cresp, err)
	}
	commit.RequestID = "commit-retry"
	commit.ActionCommit = false
	cresp, err = n.Commit(context.Background(), commit)
	if err != nil || !cresp.Ack || cresp.RequestID != "commit-retry" {
		t.Fatalf("commit retry resp=%+v err=%v", cresp, err)
	}
	got, _ := n.Read(context.Background(), 0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate commit changed outcome: got %q", got)
	}
}
