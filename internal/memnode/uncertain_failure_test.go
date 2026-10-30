package memnode_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

// Apply the operation before losing its acknowledgement. A negative Ack is
// equally ambiguous: the primary cannot assume the replica rolled anything back.
type uncertainReplica struct {
	*memnode.Participant
	phase string
	err   error
}

func (r uncertainReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	resp, err := r.Participant.ReplicateRedoEntry(ctx, req)
	if err == nil && r.phase == "redo" {
		return sinfonia.ReplicateRedoEntryResponse{}, r.err
	}
	return resp, err
}

func (r uncertainReplica) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	resp, err := r.Participant.ReplicateCommitDecision(ctx, req)
	if err == nil && r.phase == "decision" {
		return sinfonia.ReplicateCommitDecisionResponse{}, r.err
	}
	return resp, err
}

func TestUncertainPrepareAndOnePCRequireRestart(t *testing.T) {
	for _, multi := range []bool{false, true} {
		path := "one_pc"
		if multi {
			path = "prepare"
		}
		for _, failure := range []string{"redo_sync", "replica_redo_error", "replica_redo_nack", "replica_decision_error", "replica_decision_nack"} {
			if multi && (failure == "replica_decision_error" || failure == "replica_decision_nack") {
				continue // Phase-two delivery is covered separately below.
			}
			t.Run(path+"/"+failure, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				node := newRecoveredFailureTestNode(t, 1, dir)
				injected := errors.New("acknowledgement lost after persistence")
				var replica *memnode.Participant
				if failure == "redo_sync" {
					node.SetRedoFsyncHookForTest(func(f *os.File) error {
						if err := f.Sync(); err != nil {
							return err
						}
						return injected
					})
				} else {
					replica = newRecoveredFailureTestNode(t, 3, t.TempDir())
					replica.ConfigureAsReplica(1)
					peer := uncertainReplica{Participant: replica, phase: "redo", err: injected}
					if failure == "replica_decision_error" || failure == "replica_decision_nack" {
						peer.phase = "decision"
					}
					if failure == "replica_redo_nack" || failure == "replica_decision_nack" {
						peer.err = nil
					}
					node.ConfigureAsPrimary(3, peer)
				}
				other := memnode.NewParticipantWithId(2, 16)
				nodes := map[uint32]client.Participant{1: node, 2: other}
				writes := []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("A")}}
				if multi {
					writes = append(writes, sinfonia.WriteItem{MemID: 2, Addr: 0, Data: []byte("B")})
				}
				coord := client.NewCoordinator(nodes)
				result := coord.ExecuteAndCommit(ctx, "", nil, nil, writes)
				if !result.UnknownOutcome || result.Committed || result.Recovery == nil || result.TransportErr == nil {
					t.Fatalf("ambiguous persistence must survive automatic resolution as unknown: %+v", result)
				}
				if (failure == "redo_sync" || failure == "replica_redo_error" || failure == "replica_decision_error") && !errors.Is(result.TransportErr, injected) {
					t.Errorf("original failure was lost: %v", result.TransportErr)
				}
				tid := result.Recovery.TID
				assertParticipantFaulted(t, node, tid)
				if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
					t.Fatalf("primary applied unresolved write: %q, %v", got, err)
				}
				if replica != nil && (failure == "replica_decision_error" || failure == "replica_decision_nack") {
					if got, err := replica.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
						t.Fatalf("test must lose the acknowledgement after replica apply: %q, %v", got, err)
					}
				}

				// Reopen the files: Recover on the old object would miss a redo
				// record whose sync returned an error after writing it to disk.
				if err := node.Close(); err != nil {
					t.Fatal(err)
				}
				restarted, err := memnode.NewDurableParticipant(1, 16, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restarted.Close() })
				restarted.SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: other})
				if err := restarted.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
					t.Fatalf("restart lost persisted write: %q, %v", got, err)
				}
				if replica != nil {
					// Keep application traffic stopped until the replica has a
					// fresh baseline, including decisions it may have missed.
					restarted.ConfigureAsPrimary(3, replica)
					if err := replica.SyncFromPrimary(ctx, restarted); err != nil {
						t.Fatal(err)
					}
					if got, err := replica.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
						t.Fatalf("resynchronization lost committed write: %q, %v", got, err)
					}
				}
				resolved := client.NewCoordinator(map[uint32]client.Participant{1: restarted, 2: other}).Resolve(ctx, *result.Recovery)
				if resolved.UnknownOutcome || !resolved.Committed || resolved.TransportErr != nil {
					t.Fatalf("same attempt did not resolve after restart: %+v", resolved)
				}
			})
		}
	}
}

func TestRedoFailureBeforeWriteResolvesAbortAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	// Make opening the log fail before writing any bytes.
	path := filepath.Join(dir, "redo.log")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	coord := client.NewCoordinator(map[uint32]client.Participant{1: node})
	result := coord.ExecuteAndCommit(ctx, "", nil, nil, []sinfonia.WriteItem{{MemID: 1, Data: []byte("A")}})
	if !result.UnknownOutcome || result.Recovery == nil {
		t.Fatalf("expected conservative uncertainty: %+v", result)
	}
	assertParticipantFaulted(t, node, result.Recovery.TID)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoveredFailureTestNode(t, 1, dir)
	resolved := client.NewCoordinator(map[uint32]client.Participant{1: restarted}).Resolve(ctx, *result.Recovery)
	if resolved.UnknownOutcome || resolved.Committed || resolved.TransportErr != nil {
		t.Fatalf("missing redo must resolve to abort after reopening: %+v", resolved)
	}
	if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
		t.Fatalf("unlogged transaction was applied: %q, %v", got, err)
	}
}

func TestUncertainTwoPhaseDecisionRequiresRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := newRecoveredFailureTestNode(t, 1, dir)
	replica := newRecoveredFailureTestNode(t, 2, t.TempDir())
	replica.ConfigureAsReplica(1)
	node.ConfigureAsPrimary(2, uncertainReplica{Participant: replica, phase: "decision", err: context.DeadlineExceeded})
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	prep, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}}})
	if err != nil || prep.Vote != sinfonia.VoteOk {
		t.Fatalf("prepare failed: %+v, %v", prep, err)
	}
	committed, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: true})
	if committed.Ack || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost decision acknowledgement must return an error: %+v, %v", committed, err)
	}
	assertParticipantFaulted(t, node, tid)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoveredFailureTestNode(t, 1, dir)
	if got, err := restarted.ReadBytesForTest(0, 1); err != nil || string(got) != "A" {
		t.Fatalf("saved decision lost its writes: %q, %v", got, err)
	}
}

func TestReplicaRedoFailureCannotBeBypassedByPromotion(t *testing.T) {
	ctx := context.Background()
	replica := newRecoveredFailureTestNode(t, 2, t.TempDir())
	replica.ConfigureAsReplica(1)
	replica.SetRedoFsyncHookForTest(func(f *os.File) error {
		if err := f.Sync(); err != nil {
			return err
		}
		return context.DeadlineExceeded
	})
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	entry := sinfonia.ReplicateRedoEntryRequest{Entry: sinfonia.RedoEntryPayload{TID: tid, Participants: []uint32{1}, WriteItems: []sinfonia.WriteItem{{Data: []byte("A")}}}}
	if resp, err := replica.ReplicateRedoEntry(ctx, entry); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("replica append did not fault: %+v, %v", resp, err)
	}
	if resp, err := replica.ReplicateCommitDecision(ctx, sinfonia.ReplicateCommitDecisionRequest{TID: tid, ActionCommit: true}); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted replica acknowledged a decision: %+v, %v", resp, err)
	}
	if resp, err := replica.ReplicateRedoEntry(ctx, entry); resp.Ack || !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("faulted replica acknowledged another append: %+v, %v", resp, err)
	}
	replica.PromoteToPrimary(0, nil)
	status, err := replica.ReplicationStatus(ctx, sinfonia.ReplicationStatusRequest{})
	if err != nil || status.Ready {
		t.Fatalf("promotion cleared replica fault: %+v, %v", status, err)
	}
	if _, err := replica.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: tid}); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("promoted replica invented a vote: %v", err)
	}
	if err := replica.Recover(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Fatalf("same-instance recovery cleared replica fault: %v", err)
	}
}

func TestInFlightReplicationStopsWhenAnotherTransactionFaults(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(map[bool]string{false: "one_pc", true: "prepare"}[prepare], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			node := newRecoveredFailureTestNode(t, 1, t.TempDir())
			peer := newBlockingReplica()
			node.ConfigureAsPrimary(2, peer)
			tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			writes := []sinfonia.WriteItem{{Addr: 0, Data: []byte("A")}}
			done := make(chan error, 1)
			go func() {
				if prepare {
					_, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}, WriteItems: writes})
					done <- err
				} else {
					_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: tid, WriteItems: writes})
					done <- err
				}
			}()
			select {
			case <-peer.redoStarted:
			case <-ctx.Done():
				t.Fatal("request did not reach replication")
			}
			node.SetRedoFsyncHookForTest(func(*os.File) error { return context.DeadlineExceeded })
			other := tid
			other.Counter++
			_, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: other, WriteItems: []sinfonia.WriteItem{{Addr: 8, Data: []byte("B")}}})
			close(peer.releaseRedo)
			if !errors.Is(err, memnode.ErrRestartRequired) {
				t.Fatalf("second request did not fault: %v", err)
			}
			if err := <-done; !errors.Is(err, memnode.ErrRestartRequired) {
				t.Fatalf("in-flight request ignored concurrent fault: %v", err)
			}
			pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
			if err != nil || len(pending.Entries) != 2 || node.StatsSnapshot().Locks.Active != 2 {
				t.Fatalf("concurrent fault discarded pending writes or locks: %+v, %v", pending, err)
			}
			if got, err := node.ReadBytesForTest(0, 1); err != nil || string(got) != "\x00" {
				t.Fatalf("in-flight request applied after fault: %q, %v", got, err)
			}
		})
	}
}

func newRecoveredFailureTestNode(t *testing.T, id uint32, dir string) *memnode.Participant {
	t.Helper()
	node, err := memnode.NewDurableParticipant(id, 16, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if err := node.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return node
}

func assertParticipantFaulted(t *testing.T, node *memnode.Participant, tid sinfonia.TransactionID) {
	t.Helper()
	ctx := context.Background()
	ready := httptest.NewRecorder()
	node.HTTPHandler(false).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Errorf("faulted readiness = %d, want 503", ready.Code)
	}
	pending, err := node.QueryInDoubt(ctx, sinfonia.QueryInDoubtRequest{})
	if err != nil || len(pending.Entries) != 1 || pending.Entries[0].TID != tid {
		t.Fatalf("fault discarded pending transaction: %+v, %v", pending, err)
	}
	stats := node.StatsSnapshot()
	if stats.Locks.Active == 0 {
		t.Fatalf("fault released transaction locks: %+v", stats.Locks)
	}
	unknown := sinfonia.TransactionID{CoordinatorID: 42, Epoch: sinfonia.EpochAt(time.Now())}
	for _, probe := range []sinfonia.TransactionID{tid, unknown} {
		if vote, err := node.RecoveryForceAbort(ctx, sinfonia.RecoverForceAbortRequest{TID: probe}); err == nil {
			t.Errorf("faulted node invented a recovery vote: %+v", vote)
		}
		if status, err := node.QueryTidStatus(ctx, sinfonia.QueryTidStatusRequest{TID: probe}); err == nil {
			t.Errorf("faulted node reported a transaction outcome: %+v", status)
		}
		if _, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{TID: probe}); err == nil {
			t.Error("faulted node accepted a one-phase request")
		}
		if _, err := node.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: probe}); err == nil {
			t.Error("faulted node accepted a prepare")
		}
	}
	for _, commit := range []bool{false, true} {
		if _, err := node.RecoveryDecide(ctx, sinfonia.RecoverDecideRequest{TID: tid, ActionCommit: commit}); err == nil {
			t.Error("faulted node accepted a recovery decision")
		}
		if _, err := node.Commit(ctx, sinfonia.CommitRequest{TID: tid, ActionCommit: commit}); err == nil {
			t.Error("faulted node accepted a decision")
		}
	}
	if err := node.Recover(ctx); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Errorf("same-instance recovery cleared a fault without rereading storage: %v", err)
	}
	if _, err := node.ConfigureReplication(ctx, sinfonia.ConfigureReplicationRequest{Role: sinfonia.ReplicationRoleSingle}); err == nil {
		t.Error("role configuration bypassed the fault")
	}
	if err := node.LockAllBlockingOwned(ctx, "fault-test"); !errors.Is(err, memnode.ErrRestartRequired) {
		t.Errorf("faulted node allowed a backup or baseline transfer: %v", err)
	}
}
