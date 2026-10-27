package memnode_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
)

func TestParticipantRecoveryCommitsFromAllPositiveVotes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	txId := proto.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	if resp, err := a.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID: txId, Participants: participants, HasWrites: true,
		WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != proto.VoteOk {
		t.Fatalf("prepare a failed vote=%v err=%v", resp.Vote, err)
	}
	if resp, err := b.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID: txId, Participants: participants, HasWrites: true,
		WriteItems: []proto.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}},
	}); err != nil || resp.Vote != proto.VoteOk {
		t.Fatalf("prepare b failed vote=%v err=%v", resp.Vote, err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a2, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := a2.Read(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("all-positive vote recovery value=%q want=A", got)
	}
}

func TestParticipantRecoveryUsesDurableConditionalAbortDecisionDespiteClockSkew(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	now := time.Now().Add(time.Hour).Truncate(time.Second)
	deadline := now.Add(time.Minute)
	nowFn := func() time.Time { return now }
	a.SetEpochSourceForTest(time.Hour, nowFn)
	b.SetEpochSourceForTest(time.Hour, nowFn)
	txId := proto.TransactionID{CoordinatorID: 44, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	for _, prepared := range []struct {
		node   *memnode.Participant
		hostId uint32
		data   string
	}{{a, 1, "A"}, {b, 2, "B"}} {
		resp, err := prepared.node.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
			TID: txId, Participants: participants, HasWrites: true, DecisionDeadlineNanos: deadline.UnixNano(),
			DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
			WriteItems:                      []proto.WriteItem{{MemID: prepared.hostId, Addr: 0, Data: []byte(prepared.data)}},
		})
		if err != nil || resp.Vote != proto.VoteOk {
			t.Fatalf("prepare memnode %d vote=%v err=%v", prepared.hostId, resp.Vote, err)
		}
	}
	if resp, err := b.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: false, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion}); err != nil || !resp.Ack {
		t.Fatalf("abort witness resp=%+v err=%v", resp, err)
	}
	now = now.Add(-time.Hour)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a, err = memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	a.SetEpochSourceForTest(time.Hour, nowFn)
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a.Read(ctx, 0, 1)
	if err != nil || len(got) != 1 || got[0] != 0 {
		t.Fatalf("conditional abort witness was ignored: value=%q err=%v", got, err)
	}
	if !a.HasDecidedForTest(txId) || b.HasForcedAbortForTest(txId) {
		t.Fatalf("participant recovery did not use the durable abort decision")
	}
}

func TestDeadlineAbortSurvivesRedoGCAndRestartRecovery(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	originalA := a
	t.Cleanup(func() { _ = originalA.Close() })
	b, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	txId := proto.TransactionID{CoordinatorID: 45, Counter: 1, Epoch: 1}
	participants := []uint32{1, 2}
	deadline := time.Now().Add(time.Hour).UnixNano()
	for _, prepared := range []struct {
		node   *memnode.Participant
		hostId uint32
		data   string
	}{{a, 1, "A"}, {b, 2, "B"}} {
		resp, err := prepared.node.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
			TID: txId, Participants: participants, HasWrites: true, DecisionDeadlineNanos: deadline,
			DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
			WriteItems:                      []proto.WriteItem{{MemID: prepared.hostId, Addr: 0, Data: []byte(prepared.data)}},
		})
		if err != nil || resp.Vote != proto.VoteOk {
			t.Fatalf("prepare memnode %d vote=%v err=%v", prepared.hostId, resp.Vote, err)
		}
	}
	if resp, err := a.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: false, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion}); err != nil || !resp.Ack {
		t.Fatalf("abort witness resp=%+v err=%v", resp, err)
	}
	if err := a.RunRedoLogGC(ctx); err != nil {
		t.Fatal(err)
	}
	if a.RedoEntryCountForTest() != 0 || !a.HasForcedAbortForTest(txId) || !a.HasDecidedForTest(txId) {
		t.Fatalf("deadline abort GC did not retain forced-abort evidence")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := memnode.NewDurableParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	epochNow := time.Now()
	restarted.SetEpochSourceForTest(time.Hour, func() time.Time { return epochNow })
	if !restarted.HasForcedAbortForTest(txId) || !restarted.HasDecidedForTest(txId) {
		t.Fatal("deadline abort evidence did not survive restart")
	}
	late, err := restarted.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: true, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion})
	if err != nil || late.Ack {
		t.Fatalf("late commit after deadline-abort GC resp=%+v err=%v", late, err)
	}

	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: restarted})
	if err := b.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Read(ctx, 0, 1); err != nil || len(got) != 1 || got[0] != 0 {
		t.Fatalf("deadline-aborted peer recovered as committed: value=%q err=%v", got, err)
	}
	epochNow = epochNow.Add(3 * time.Hour)
	if err := restarted.RunForcedAbortGC(); err != nil {
		t.Fatal(err)
	}
	if restarted.HasForcedAbortForTest(txId) || restarted.HasDecidedForTest(txId) {
		t.Fatal("deadline abort evidence survived its safe epoch-GC horizon")
	}
}

func TestLateCommitDoesNotAcknowledgeForcedOrFinalAbort(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		abort func(*testing.T, *memnode.Participant, proto.TransactionID)
	}{
		{
			name: "forced abort",
			abort: func(t *testing.T, node *memnode.Participant, txId proto.TransactionID) {
				t.Helper()
				if err := node.ForceAbortForTest(txId); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "final abort",
			abort: func(t *testing.T, node *memnode.Participant, txId proto.TransactionID) {
				t.Helper()
				if resp, err := node.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: false, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion}); err != nil || !resp.Ack {
					t.Fatalf("abort resp=%+v err=%v", resp, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := memnode.NewDurableParticipant(1, 64, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })
			txId := proto.TransactionID{CoordinatorID: 80, Counter: 1, Epoch: 1}
			if resp, err := node.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
				TID: txId, Participants: []uint32{1, 2}, HasWrites: true,
				DecisionDeadlineNanos: time.Now().Add(time.Hour).UnixNano(), DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
				WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
			}); err != nil || resp.Vote != proto.VoteOk {
				t.Fatalf("prepare resp=%+v err=%v", resp, err)
			}
			tc.abort(t, node, txId)

			resp, err := node.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: true, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Ack {
				t.Fatalf("late commit acknowledged after %s: %+v", tc.name, resp)
			}
			status, err := node.QueryTidStatus(ctx, proto.QueryTidStatusRequest{TID: txId})
			if err != nil {
				t.Fatal(err)
			}
			if status.Outcome == proto.TidOutcomeCommit || status.VotedOK {
				t.Fatalf("late commit installed commit evidence after %s: %+v", tc.name, status)
			}
			if got, _ := node.Read(ctx, 0, 1); got[0] != 0 {
				t.Fatalf("late commit applied after %s: %q", tc.name, got)
			}
		})
	}
}

func TestForcedAbortRemainsAuthoritativeAcrossClockReversalAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	before := time.Unix(1_000, 0)
	now := before
	node.SetEpochSourceForTest(time.Hour, func() time.Time { return now })
	txId := proto.TransactionID{CoordinatorID: 81, Counter: 1, Epoch: 1}
	deadline := before.Add(time.Minute)
	if resp, err := node.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID: txId, Participants: []uint32{1, 2}, HasWrites: true, DecisionDeadlineNanos: deadline.UnixNano(),
		DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
		WriteItems:                      []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != proto.VoteOk {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	now = deadline.Add(time.Minute)
	if err := node.ForceAbortForTest(txId); err != nil {
		t.Fatal(err)
	}
	now = before.Add(-time.Hour)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewDurableParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	node.SetEpochSourceForTest(time.Hour, func() time.Time { return now })

	vote, err := node.RecoveryForceAbort(ctx, proto.RecoverForceAbortRequest{TID: txId, Epoch: txId.Epoch, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if vote.Vote != proto.VoteBadForced {
		t.Fatalf("clock reversal replaced durable forced abort with vote %v", vote.Vote)
	}
	commit, err := node.Commit(ctx, proto.CommitRequest{TID: txId, ActionCommit: true, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if commit.Ack {
		t.Fatalf("clock reversal allowed commit acknowledgement: %+v", commit)
	}
}

func TestLegacyRecoveryManagerCannotDriveDeadlineTransaction(t *testing.T) {
	ctx := context.Background()
	node, err := memnode.NewDurableParticipant(1, 64, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	txId := proto.TransactionID{CoordinatorID: 82, Counter: 1, Epoch: 1}
	if resp, err := node.ExecAndPrepare(ctx, proto.ExecPrepareRequest{
		TID: txId, Participants: []uint32{1, 2}, HasWrites: true, DecisionDeadlineNanos: time.Now().Add(time.Hour).UnixNano(),
		DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
		WriteItems:                      []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != proto.VoteOk {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	if resp, err := node.RecoveryForceAbort(ctx, proto.RecoverForceAbortRequest{TID: txId, Epoch: txId.Epoch}); err == nil || resp.Vote != proto.VoteBadInternal {
		t.Fatalf("legacy recovery request did not fail closed: resp=%+v err=%v", resp, err)
	}
}

type legacyDecisionReplica struct{}

func (legacyDecisionReplica) ReplicateRedoEntry(context.Context, proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	return proto.ReplicateRedoEntryResponse{Ack: true}, nil
}

func (legacyDecisionReplica) ReplicateCommitDecision(context.Context, proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	return proto.ReplicateCommitDecisionResponse{Ack: true}, nil
}

func (legacyDecisionReplica) ReplicateReplayRecord(context.Context, proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	return proto.ReplicateReplayRecordResponse{Ack: true}, nil
}

func TestPrimaryDeadlineCapabilityIncludesReplicaSupport(t *testing.T) {
	primary, err := memnode.NewDurableParticipant(1, 64, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	primary.ConfigureAsPrimary(2, legacyDecisionReplica{})

	capabilities, err := primary.DescribeTransactionCapabilities(context.Background(), proto.DescribeTransactionCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.DeadlineDecisionProtocolVersion != 0 {
		t.Fatalf("primary advertised deadline protocol without replica support: %+v", capabilities)
	}
}
