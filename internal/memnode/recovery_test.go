package memnode_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

// Recovery decisions from peer evidence, in-flight work, and readiness gating.

func TestReadOnlyPrepareRecovery(t *testing.T) {
	for _, kind := range []string{"read", "compare-only"} {
		for _, peerPrepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/peer-prepared=%t", kind, peerPrepared), func(t *testing.T) {
				ctx := t.Context()
				aDir := t.TempDir()
				a := newRecoveredFailureTestNode(t, 1, aDir)
				b := newRecoveredFailureTestNode(t, 2, t.TempDir())
				tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
				request := func(id uint32, expected byte) sinfonia.ExecPrepareRequest {
					req := sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}}
					if kind == "read" {
						req.ReadItems = []sinfonia.ReadItem{{MemID: id, Len: 1}}
					} else {
						req.CmpItems = []sinfonia.CmpItem{{MemID: id, Data: []byte{expected}}}
					}
					return req
				}
				first, err := a.ExecAndPrepare(ctx, request(1, 0))
				if err != nil || first.Vote != sinfonia.VoteOk {
					t.Fatalf("first prepare = %+v, %v", first, err)
				}
				if kind == "read" && !bytes.Equal(first.ReadData[0], []byte{0}) {
					t.Fatalf("first read = %v, want 0", first.ReadData)
				}
				if peerPrepared {
					if resp, err := b.ExecAndPrepare(ctx, request(2, 0)); err != nil || resp.Vote != sinfonia.VoteOk {
						t.Fatalf("peer prepare = %+v, %v", resp, err)
					}
				}

				if err := a.Close(); err != nil {
					t.Fatal(err)
				}
				restarted, err := memnode.NewParticipant(1, 16, aDir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restarted.Close() })
				// Even an empty write set needs peer evidence before new execution.
				if err := restarted.Recover(ctx); !errors.Is(err, memnode.ErrRecoveryPending) {
					t.Fatalf("recovery without peer evidence = %v, want pending", err)
				}
				assertParticipantReady(t, restarted, false)
				restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
				b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: restarted})
				if err := restarted.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				wantOutcome := sinfonia.TidOutcomeAbort
				if peerPrepared {
					wantOutcome = sinfonia.TidOutcomeCommit
				}
				status, err := restarted.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: tid})
				if err != nil || !status.KnownDecision || status.Outcome != wantOutcome {
					t.Fatalf("recovered outcome = %+v, %v; want %v", status, err, wantOutcome)
				}

				if peerPrepared {
					if resp, err := b.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
						t.Fatalf("finish peer commit = %+v, %v", resp, err)
					}
				} else {
					// The original coordinator still has A's old yes vote. Change
					// both values before delivering its delayed request to B.
					coord := client.NewCoordinator(map[uint32]client.Participant{1: restarted, 2: b})
					defer coord.Close()
					result := coord.ExecuteAndCommit(ctx, nil, nil, []sinfonia.WriteItem{
						{MemID: 1, Data: []byte{1}}, {MemID: 2, Data: []byte{1}},
					})
					if !result.Committed {
						t.Fatalf("update after recovery = %+v", result)
					}
					if err := coord.FlushPendingCommits(ctx); err != nil {
						t.Fatal(err)
					}
					// A successful read would yield (0, 1); successful compares
					// would validate values that never existed together.
					if resp, err := b.ExecAndPrepare(ctx, request(2, 1)); err != nil || resp.Vote != sinfonia.VoteBadForced {
						t.Fatalf("delayed prepare = %+v, %v; want forced abort", resp, err)
					}
				}

				// Empty-write commits use the same receipt and cleanup protocol.
				restarted.FlushAppliedNotifications(ctx)
				b.FlushAppliedNotifications(ctx)
				for _, node := range []*memnode.Participant{restarted, b} {
					if err := node.RunRedoLogGC(ctx); err != nil {
						t.Fatal(err)
					}
					if node.RedoEntryCountForTest() != 0 || node.HasDecidedForTest(tid) || node.ActiveLockCountForTest() != 0 {
						t.Fatal("completed transaction retained redo, a decision, or locks")
					}
				}
			})
		}
	}
}

func TestDurabilityCrashAfterPrepareBeforeCommitApply(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	aDir := filepath.Join(dir, "a")
	bDir := filepath.Join(dir, "b")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	b.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})

	recoverTestParticipants(t, a, b)
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}
	b.FlushAppliedNotifications(ctx)

	_ = a.Close()
	a2, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	a2.FlushAppliedNotifications(ctx)
	got, err := a2.ReadBytesForTest(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("expected recovered write to be applied, got %q", string(got))
	}
}

func TestRecoveryRejectsTornCommitRecord(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	aDir, bDir := filepath.Join(root, "a"), filepath.Join(root, "b")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	for _, participant := range []struct {
		node *memnode.Participant
		id   uint32
	}{{a, 1}, {b, 2}} {
		if err := participant.node.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		resp, err := participant.node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
			TID: tid, Participants: []uint32{1, 2},
			WriteItems: []sinfonia.WriteItem{{MemID: participant.id, Addr: 0, Data: []byte{7}}},
		})
		if err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare participant %d: response=%+v err=%v", participant.id, resp, err)
		}
	}
	if resp, err := a.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true}); err != nil || !resp.Ack {
		t.Fatalf("commit participant A: response=%+v err=%v", resp, err)
	}
	if got, err := a.ReadBytesForTest(0, 1); err != nil || !bytes.Equal(got, []byte{7}) {
		t.Fatalf("participant A did not apply its commit: value=%v err=%v", got, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a torn append by copying A's decision into B's log with the
	// final "commit\n" cut short to "comm". This is file corruption injection,
	// not a physical power-loss test; B's prepared redo remains intact.
	record, err := os.ReadFile(filepath.Join(aDir, "decided.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(record, []byte(" commit\n")) {
		t.Fatalf("expected a complete commit record, got %q", record)
	}
	if err := os.WriteFile(filepath.Join(bDir, "decided.log"), bytes.TrimSuffix(record, []byte("it\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	// Malformed records must block startup, either while opening storage or
	// during recovery. They must never become authoritative abort evidence.
	// Manual repair is acceptable for now. Future recovery could automate safe
	// torn-tail repair using intact redo and peer decision evidence.
	restarted, err := memnode.NewParticipant(2, 64, bDir)
	if err != nil {
		return
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: a})
	recoveryErr := restarted.Recover(ctx)
	status, statusErr := restarted.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: tid})
	if statusErr == nil && status.KnownDecision && status.Outcome == sinfonia.TidOutcomeAbort {
		t.Errorf("torn commit record became a known abort: %+v", status)
	}
	if recoveryErr == nil {
		aValue, aErr := a.ReadBytesForTest(0, 1)
		bValue, bErr := restarted.ReadBytesForTest(0, 1)
		t.Fatalf("recovery accepted a torn commit record: A=%v (err=%v), B=%v (err=%v), B status=%+v (err=%v); want startup corruption error",
			aValue, aErr, bValue, bErr, status, statusErr)
	}
	assertParticipantReady(t, restarted, false)
}

func TestRecoveryOutcomeDiscoveryFromPeerDecision(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	aDir := filepath.Join(dir, "a")
	bDir := filepath.Join(dir, "b")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	recoverTestParticipants(t, a, b)
	txId := sinfonia.TransactionID{CoordinatorID: 7, Counter: 4, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 5, Data: []byte("K")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants, WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 5, Data: []byte("Z")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil {
		t.Fatal(err)
	}

	_ = a.Close()
	a2, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a2.ReadBytesForTest(5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) == "K" {
		t.Fatalf("expected write not applied when peer says abort")
	}
}

func TestParticipantRecoveryCommitsFromAllPositiveVotes(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	recoverTestParticipants(t, a, b)
	txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if resp, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: participants,
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare a failed vote=%v err=%v", resp.Vote, err)
	}
	if resp, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: participants,
		WriteItems: []sinfonia.WriteItem{{MemID: 2, Addr: 0, Data: []byte("B")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare b failed vote=%v err=%v", resp.Vote, err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a2, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Close() })
	a2.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	// Persist the peer receipt before TempDir cleanup; background duplicates
	// then need no disk writes after this test finishes.
	a2.FlushAppliedNotifications(ctx)

	got, err := a2.ReadBytesForTest(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("all-positive vote recovery value=%q want=A", got)
	}
}

func TestParticipantRecoveryUsesDurableAbortDecision(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := memnode.NewParticipant(2, 64, filepath.Join(root, "b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	recoverTestParticipants(t, a, b)
	txId := sinfonia.TransactionID{CoordinatorID: 44, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	for _, prepared := range []struct {
		node   *memnode.Participant
		hostId uint32
		data   string
	}{{a, 1, "A"}, {b, 2, "B"}} {
		resp, err := prepared.node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
			TID: txId, Participants: participants,
			WriteItems: []sinfonia.WriteItem{{MemID: prepared.hostId, Addr: 0, Data: []byte(prepared.data)}},
		})
		if err != nil || resp.Vote != sinfonia.VoteOk {
			t.Fatalf("prepare memnode %d vote=%v err=%v", prepared.hostId, resp.Vote, err)
		}
	}
	if resp, err := b.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil || !resp.Ack {
		t.Fatalf("abort witness resp=%+v err=%v", resp, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a, err = memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	a.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := a.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := a.ReadBytesForTest(0, 1)
	if err != nil || len(got) != 1 || got[0] != 0 {
		t.Fatalf("durable abort decision was ignored: value=%q err=%v", got, err)
	}
	if !a.HasDecidedForTest(txId) || b.HasForcedAbortForTest(txId) {
		t.Fatalf("participant recovery did not use the durable abort decision")
	}
}

func TestLateCommitDoesNotAcknowledgeForcedOrFinalAbort(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name  string
		abort func(*testing.T, *memnode.Participant, sinfonia.TransactionID)
	}{
		{
			name: "forced abort",
			abort: func(t *testing.T, node *memnode.Participant, txId sinfonia.TransactionID) {
				t.Helper()
				if err := node.ForceAbortForTest(txId); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "final abort",
			abort: func(t *testing.T, node *memnode.Participant, txId sinfonia.TransactionID) {
				t.Helper()
				if resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: false}); err != nil || !resp.Ack {
					t.Fatalf("abort resp=%+v err=%v", resp, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := memnode.NewParticipant(1, 64, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })
			recoverTestParticipants(t, node)
			txId := sinfonia.TransactionID{CoordinatorID: 80, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID: txId, Participants: []uint32{1, 2},
				WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
			}); err != nil || resp.Vote != sinfonia.VoteOk {
				t.Fatalf("prepare resp=%+v err=%v", resp, err)
			}
			tc.abort(t, node, txId)

			resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Ack {
				t.Fatalf("late commit acknowledged after %s: %+v", tc.name, resp)
			}
			status, err := node.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: txId})
			if err != nil {
				t.Fatal(err)
			}
			if status.Outcome == sinfonia.TidOutcomeCommit || status.VotedOK {
				t.Fatalf("late commit installed commit evidence after %s: %+v", tc.name, status)
			}
			if got, _ := node.ReadBytesForTest(0, 1); got[0] != 0 {
				t.Fatalf("late commit applied after %s: %q", tc.name, got)
			}
		})
	}
}

func TestForcedAbortRemainsAuthoritativeAcrossClockReversalAndRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	node, err := memnode.NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	before := time.Unix(3600, 0)
	now := before
	node.SetClockForTest(func() time.Time { return now })
	recoverTestParticipants(t, node)
	txId := sinfonia.TransactionID{CoordinatorID: 81, Counter: 1, Epoch: sinfonia.EpochAt(before)}
	if resp, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: txId, Participants: []uint32{1, 2},
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare resp=%+v err=%v", resp, err)
	}
	now = before.Add(2 * time.Hour)
	if err := node.ForceAbortForTest(txId); err != nil {
		t.Fatal(err)
	}
	now = before.Add(-time.Hour)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = memnode.NewParticipant(1, 64, dir)
	if err != nil {
		t.Fatal(err)
	}
	node.SetClockForTest(func() time.Time { return now })

	vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: txId, Epoch: txId.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if vote.Vote != sinfonia.VoteBadForced {
		t.Fatalf("clock reversal replaced durable forced abort with vote %v", vote.Vote)
	}
	commit, err := node.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	if commit.Ack {
		t.Fatalf("clock reversal allowed commit acknowledgement: %+v", commit)
	}
}

func TestRecoveryAbortRejectsLateOnePhaseRequest(t *testing.T) {
	node := newTestParticipant(t, 1, 16)
	ctx := t.Context()
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

type unavailableRecoveryPeer struct{}

func (unavailableRecoveryPeer) RecoveryForceAbort(ctx context.Context, _ sinfonia.RecoverForceAbortRequest) (sinfonia.RecoverForceAbortResponse, error) {
	<-ctx.Done()
	return sinfonia.RecoverForceAbortResponse{}, ctx.Err()
}

func (unavailableRecoveryPeer) RecoveryDecide(context.Context, sinfonia.RecoverDecideRequest) (sinfonia.RecoverDecideResponse, error) {
	return sinfonia.RecoverDecideResponse{}, errors.New("peer unavailable")
}

func (unavailableRecoveryPeer) NotifyApplied(context.Context, sinfonia.NotifyAppliedRequest) (sinfonia.NotifyAppliedResponse, error) {
	return sinfonia.NotifyAppliedResponse{}, errors.New("peer is still recovering")
}

func TestStartupRecoveryUsesAvailableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		decision    string
		unavailable bool
		wantPending bool
	}{
		{name: "commit/missing peer", decision: "commit"},
		{name: "abort/missing peer", decision: "abort"},
		{name: "commit/unreachable peer", decision: "commit", unavailable: true},
		{name: "abort/unreachable peer", decision: "abort", unavailable: true},
		{name: "no vote/unreachable peer", decision: "never voted", unavailable: true},
		{name: "yes votes/unreachable peer", unavailable: true, wantPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			local := newRecoveredFailureTestNode(t, 1, dir)
			peer := newTestParticipant(t, 2, 16)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			// Put the unavailable participant before the useful evidence.
			request := sinfonia.ExecPrepareRequest{
				TID: tid, Participants: []uint32{1, 3, 2},
				WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}},
			}
			for _, node := range []*memnode.Participant{local, peer} {
				if node == peer && tc.decision == "never voted" {
					continue
				}
				if resp, err := node.ExecAndPrepare(ctx, request); err != nil || resp.Vote != sinfonia.VoteOk {
					t.Fatalf("prepare = %+v, %v", resp, err)
				}
			}
			if tc.decision == "commit" || tc.decision == "abort" {
				if resp, err := peer.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: tc.decision == "commit"}); err != nil || !resp.Ack {
					t.Fatalf("peer decision = %+v, %v", resp, err)
				}
			}
			if err := local.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := memnode.NewParticipant(1, 16, dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			peers := map[uint32]memnode.RecoveryPeer{2: peer}
			if tc.unavailable {
				peers[3] = unavailableRecoveryPeer{}
			}
			restarted.SetRecoveryPeers(peers)
			// Discovery must leave application to the ordered replay loop.
			restarted.SetRecoverBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
				if phase == "apply-before" {
					image, err := os.ReadFile(filepath.Join(dir, "image.bin"))
					if err != nil {
						return err
					}
					if image[0] != 0 {
						return errors.New("discovery applied the write before ordered replay")
					}
				}
				return nil
			})
			attempt, cancel := context.WithTimeout(ctx, time.Second)
			err = restarted.Recover(attempt)
			cancel()
			if tc.wantPending {
				if !errors.Is(err, memnode.ErrRecoveryPending) || restarted.HasDecidedForTest(tid) {
					t.Fatalf("insufficient evidence = %v; must stay undecided", err)
				}
			} else if err != nil {
				t.Fatalf("sufficient evidence did not finish startup: %v", err)
			}
			assertParticipantReady(t, restarted, !tc.wantPending)
			want := byte(0)
			if tc.decision == "commit" {
				want = 'A'
			}
			if got, err := restarted.ReadBytesForTest(0, 1); err != nil || got[0] != want {
				t.Fatalf("recovered data = %q, %v; want %q", got, err, want)
			}
			// Drain receipt writes before temporary-directory cleanup.
			restarted.FlushAppliedNotifications(ctx)
		})
	}
}

func TestParticipantRecoveryWaitsForMissingPeerEvidence(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	bDir := filepath.Join(root, "b")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := memnode.NewParticipant(2, 64, bDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	recoverTestParticipants(t, a, b)
	txId := sinfonia.TransactionID{CoordinatorID: 41, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	assertParticipantReady(t, restarted, false)

	err = restarted.Recover(ctx)
	if !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v, want ErrRecoveryPending", err)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("missing peer evidence must not persist an abort decision")
	}
	processed, err := storage.LoadProcessedPointer(aDir)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("processed pointer = %d, want 0 while recovery is pending", processed)
	}
	assertParticipantReady(t, restarted, false)

	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	applyBlocked := make(chan struct{})
	releaseApply := make(chan struct{})
	restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID sinfonia.TransactionID) error {
		if phase == "apply-before" && hookTID == txId {
			close(applyBlocked)
			<-releaseApply
		}
		return nil
	})
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- restarted.Recover(ctx) }()
	select {
	case <-applyBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not reach apply boundary")
	}
	decisionPath := filepath.Join(aDir, "decided.log")
	decisionBefore, err := os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	type commitResult struct {
		resp sinfonia.CommitResponse
		err  error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{TID: txId, RequestID: "opposite-race", ActionCommit: false})
		commitDone <- commitResult{resp: resp, err: err}
	}()
	select {
	case result := <-commitDone:
		t.Fatalf("concurrent Commit returned before recovery terminalization: resp=%+v err=%v", result.resp, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseApply)
	if err := <-recoverDone; err != nil {
		t.Fatal(err)
	}
	result := <-commitDone
	if result.err != nil || !result.resp.Ack {
		t.Fatalf("opposite phase-two race resp=%+v err=%v", result.resp, result.err)
	}
	decisionAfter, err := os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decisionBefore, decisionAfter) {
		t.Fatalf("opposite phase-two race changed recovery decision: before=%q after=%q", decisionBefore, decisionAfter)
	}
	got, err := restarted.ReadBytesForTest(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Fatalf("recovered value = %q, want A", got)
	}
	decisionBefore, err = os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApplyWriteForTest(8, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{TID: txId, RequestID: "same-tid-retry", ActionCommit: true})
	if err != nil || !resp.Ack {
		t.Fatalf("same-TID commit retry resp=%+v err=%v", resp, err)
	}
	decisionAfter, err = os.ReadFile(decisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decisionBefore, decisionAfter) {
		t.Fatalf("same-TID retry mutated decided state: before=%q after=%q", decisionBefore, decisionAfter)
	}
	got, err = restarted.ReadBytesForTest(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Z" {
		t.Fatalf("same-TID retry reapplied old write: got %q want Z", got)
	}
	assertParticipantReady(t, restarted, true)
}

func TestParticipantRecoveryFailurePreservesDurableDecision(t *testing.T) {
	for _, failurePhase := range []string{"apply-before", "apply-after", "pointer-before"} {
		t.Run(failurePhase, func(t *testing.T) {
			ctx := t.Context()
			root := t.TempDir()
			aDir := filepath.Join(root, "a")
			bDir := filepath.Join(root, "b")
			a, err := memnode.NewParticipant(1, 64, aDir)
			if err != nil {
				t.Fatal(err)
			}
			b, err := memnode.NewParticipant(2, 64, bDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })

			recoverTestParticipants(t, a, b)
			txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			participants := []uint32{1, 2}
			if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
				TID:          txId,
				Participants: participants,
				WriteItems:   []sinfonia.WriteItem{{MemID: 2, Addr: 8, Data: []byte("B")}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := memnode.NewParticipant(1, 64, aDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
			injected := errors.New("injected recovery terminalization failure")
			restarted.SetRecoverBoundaryHookForTest(func(phase string, hookTID sinfonia.TransactionID) error {
				if phase == failurePhase && hookTID == txId {
					return injected
				}
				return nil
			})
			if err := restarted.Recover(ctx); !errors.Is(err, injected) {
				t.Fatalf("Recover() error = %v, want %v", err, injected)
			}

			decisionPath := filepath.Join(aDir, "decided.log")
			decisionBefore, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := restarted.Commit(ctx, sinfonia.CommitRequest{
				TID:          txId,
				RequestID:    "opposite-after-recovery-failure",
				ActionCommit: false,
			})
			if err != nil || !resp.Ack {
				t.Fatalf("opposite Commit resp=%+v err=%v", resp, err)
			}
			decisionAfter, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decisionBefore, decisionAfter) {
				t.Fatalf("opposite Commit replaced durable recovery decision: before=%q after=%q", decisionBefore, decisionAfter)
			}
			got, err := restarted.ReadBytesForTest(8, 1)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "A" {
				t.Fatalf("normalized Commit value = %q, want A", got)
			}

			restarted.SetRecoverBoundaryHookForTest(nil)
			if err := restarted.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			decisionAfterRetry, err := os.ReadFile(decisionPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decisionBefore, decisionAfterRetry) {
				t.Fatalf("recovery retry replaced durable decision: before=%q after=%q", decisionBefore, decisionAfterRetry)
			}
		})
	}
}

func TestParticipantRecoveryRetriesUnavailablePeer(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	aDir := filepath.Join(root, "a")
	a, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	b := newTestParticipant(t, 2, 64)

	recoverTestParticipants(t, a)
	txId := sinfonia.TransactionID{CoordinatorID: 42, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	participants := []uint32{1, 2}
	if _, err := a.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID:          txId,
		Participants: participants,
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 9, Data: []byte("R")}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: participants}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := memnode.NewParticipant(1, 64, aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: unavailableRecoveryPeer{}})
	attempt, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err = restarted.Recover(attempt)
	cancel()
	if !errors.Is(err, memnode.ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v, want ErrRecoveryPending", err)
	}
	if restarted.HasDecidedForTest(txId) {
		t.Fatal("unreachable peer must not persist an abort decision")
	}

	restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: b})
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ReadBytesForTest(9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "R" {
		t.Fatalf("recovered value = %q, want R", got)
	}
}

func TestRPCRejectsClientWorkUntilRecoveryCompletes(t *testing.T) {
	node, err := memnode.NewParticipant(1, 64, filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = node.RPCServer().Serve(listener) }()

	client := rpcwire.NewClient(listener.Addr().String(), rpcwire.ClientOptions{DialTimeout: time.Second})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var read sinfonia.ExecCommit1PCResponse
	err = client.Call(ctx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 17, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}, ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}}}, &read)
	if err == nil || !strings.Contains(err.Error(), memnode.ErrRecoveryPending.Error()) {
		t.Fatalf("Read before recovery error = %v, want ErrRecoveryPending", err)
	}

	var entries sinfonia.QueryInDoubtResponse
	if err := client.Call(ctx, sinfonia.MethodQueryInDoubt, sinfonia.QueryInDoubtRequest{}, &entries); err != nil {
		t.Fatalf("recovery-safe RPC failed while unready: %v", err)
	}

	if err := node.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(ctx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{TID: sinfonia.TransactionID{CoordinatorID: 17, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}, ReadItems: []sinfonia.ReadItem{{Addr: 0, Len: 1}}}, &read); err != nil {
		t.Fatalf("Read after recovery failed: %v", err)
	}
}

func TestExecutionCannotOvertakeRecoverySnapshot(t *testing.T) {
	for _, onePC := range []bool{false, true} {
		t.Run(fmt.Sprintf("onePC=%t", onePC), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			node := newRecoveredFailureTestNode(t, 1, t.TempDir())
			original := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			execute := func(tid sinfonia.TransactionID, value string) error {
				writes := []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte(value)}}
				var vote sinfonia.Vote
				var err error
				if onePC {
					resp, callErr := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: writes})
					vote, err = resp.Vote, callErr
				} else {
					resp, callErr := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: writes})
					vote, err = resp.Vote, callErr
				}
				if err != nil {
					return err
				}
				if vote != sinfonia.VoteOk {
					return fmt.Errorf("execution vote = %v", vote)
				}
				if !onePC {
					resp, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true})
					if err != nil || !resp.Ack {
						return fmt.Errorf("commit = %+v, %v", resp, err)
					}
				}
				return nil
			}
			if err := execute(original, "old"); err != nil {
				t.Fatal(err)
			}
			// An unrelated image failure makes this same participant need recovery.
			fault := errors.New("injected image write failure")
			node.SetWriteApplyFaultForTest(func(uint64, []byte) error { return fault })
			failed := original
			failed.Counter++
			if _, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
				TID: failed, WriteItems: []sinfonia.WriteItem{{Addr: 8, Data: []byte("X")}},
			}); !errors.Is(err, fault) {
				t.Fatalf("image failure = %v, want %v", err, fault)
			}
			node.SetWriteApplyFaultForTest(nil)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			node.SetRecoverBoundaryHookForTest(func(phase string, tid sinfonia.TransactionID) error {
				if phase == "replay-before" && tid == original {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			done := make(chan struct{})
			var recoveryErr error
			go func() {
				defer close(done)
				recoveryErr = node.Recover(ctx)
			}()
			defer func() { unblock(); <-done }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Recovery has its redo snapshot but holds no mutex here. Both new
			// execution and cached replies must remain gated inside execution.
			fresh := original
			fresh.Counter += 2
			if err := execute(fresh, "new"); !errors.Is(err, memnode.ErrRecoveryPending) {
				t.Errorf("new execution during recovery = %v, want ErrRecoveryPending", err)
			}
			if err := execute(original, "old"); !errors.Is(err, memnode.ErrRecoveryPending) {
				t.Errorf("cached execution during recovery = %v, want ErrRecoveryPending", err)
			}
			unblock()
			<-done
			if recoveryErr != nil {
				t.Fatal(recoveryErr)
			}
			node.SetRecoverBoundaryHookForTest(nil)
			if err := execute(fresh, "new"); err != nil {
				t.Fatalf("execution after recovery = %v", err)
			}
			if got, err := node.ReadBytesForTest(0, 3); err != nil || string(got) != "new" {
				t.Fatalf("acknowledged write = %q, %v; want new", got, err)
			}
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if got, err := node.ReadBytesForTest(0, 3); err != nil || string(got) != "new" {
				t.Fatalf("recovered acknowledged write = %q, %v; want new", got, err)
			}
		})
	}
}

func TestRecoveryAndDuplicateWaitForDurableVote(t *testing.T) {
	for _, onePC := range []bool{false, true} {
		t.Run(fmt.Sprintf("onePC=%t", onePC), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			node := newRecoveredFailureTestNode(t, 1, t.TempDir())
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			node.SetRedoFsyncHookForTest(func(f *os.File) error {
				close(entered)
				select {
				case <-release:
					return f.Sync()
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			execute := func(requestID sinfonia.RequestID) error {
				reads := []sinfonia.ReadItem{{MemID: 1, Len: 1}}
				writes := []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}}
				var vote sinfonia.Vote
				var data map[uint32][]byte
				var err error
				if onePC {
					resp, callErr := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, RequestID: requestID, ReadItems: reads, WriteItems: writes})
					vote, data, err = resp.Vote, resp.ReadData, callErr
				} else {
					resp, callErr := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, RequestID: requestID, Participants: []uint32{1}, ReadItems: reads, WriteItems: writes})
					vote, data, err = resp.Vote, resp.ReadData, callErr
				}
				if err != nil || vote != sinfonia.VoteOk || string(data[0]) != "\x00" {
					return fmt.Errorf("%s: vote=%v data=%q err=%v", requestID, vote, data[0], err)
				}
				return nil
			}
			original := make(chan error, 1)
			go func() { original <- execute("original") }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			results := make(chan error, 2)
			go func() { results <- execute("duplicate") }()
			go func() {
				vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid})
				if err == nil && vote.Vote != sinfonia.VoteOk {
					err = fmt.Errorf("recovery changed the vote: %+v", vote)
				}
				results <- err
			}()
			select {
			case err := <-results:
				t.Fatalf("request bypassed pending persistence: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			unblock()
			if err := <-original; err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
			if node.HasForcedAbortForTest(tid) {
				t.Fatal("recovery persisted a conflicting abort")
			}
		})
	}
}
