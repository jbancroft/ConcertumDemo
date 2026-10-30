package memnode

import (
	"context"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestExecCommit1PCDuplicateTIDReturnsOriginalOutcomeWithoutReapply(t *testing.T) {
	n := NewParticipant(8)
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	req := sinfonia.ExecCommit1PCRequest{TID: txId, RequestID: "first", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecCommit1PC(context.Background(), req)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("first 1pc resp=%+v err=%v", resp, err)
	}
	retry := req
	retry.RequestID = "retry"
	retry.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecCommit1PC(context.Background(), retry)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("retry 1pc should return original outcome, resp=%+v err=%v", resp, err)
	}
	got, _ := n.ReadBytesForTest(0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate 1pc reapplied write: got %q", got)
	}
}

func TestPrepareAndCommitDuplicateTIDReturnOriginalOutcomeAfterResponseLoss(t *testing.T) {
	n := NewParticipant(8)
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	prep := sinfonia.ExecPrepareRequest{TID: txId, RequestID: "prepare-1", Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 1}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}
	resp, err := n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != sinfonia.VoteOk || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	prep.RequestID = "prepare-retry"
	prep.WriteItems[0].Data = []byte("B")
	resp, err = n.ExecAndPrepare(context.Background(), prep)
	if err != nil || resp.Vote != sinfonia.VoteOk || resp.RequestID != "prepare-retry" || string(resp.ReadData[0]) != "\x00" {
		t.Fatalf("prepare retry resp=%+v err=%v", resp, err)
	}
	commit := sinfonia.CommitRequest{TID: txId, RequestID: "commit-1", ActionCommit: true}
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
	got, _ := n.ReadBytesForTest(0, 1)
	if string(got) != "A" {
		t.Fatalf("duplicate commit changed outcome: got %q", got)
	}
}
