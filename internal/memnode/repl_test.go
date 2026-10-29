package memnode_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type slowReplica struct {
	node  *memnode.Participant
	delay time.Duration
}

func (s *slowReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateRedoEntry(ctx, req)
}

func (s *slowReplica) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateCommitDecision(ctx, req)
}

func (s *slowReplica) ReplicateReplayRecord(ctx context.Context, req sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateReplayRecord(ctx, req)
}

type failingReplica struct{}

func (f failingReplica) ReplicateRedoEntry(context.Context, sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	return sinfonia.ReplicateRedoEntryResponse{Ack: false}, context.DeadlineExceeded
}

func (f failingReplica) ReplicateCommitDecision(context.Context, sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	return sinfonia.ReplicateCommitDecisionResponse{Ack: false}, context.DeadlineExceeded
}

func (f failingReplica) ReplicateReplayRecord(context.Context, sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	return sinfonia.ReplicateReplayRecordResponse{Ack: false}, context.DeadlineExceeded
}

type replayFailingReplica struct {
	node *memnode.Participant
}

func (r replayFailingReplica) ReplicateRedoEntry(ctx context.Context, req sinfonia.ReplicateRedoEntryRequest) (sinfonia.ReplicateRedoEntryResponse, error) {
	return r.node.ReplicateRedoEntry(ctx, req)
}

func (r replayFailingReplica) ReplicateCommitDecision(ctx context.Context, req sinfonia.ReplicateCommitDecisionRequest) (sinfonia.ReplicateCommitDecisionResponse, error) {
	return r.node.ReplicateCommitDecision(ctx, req)
}

func (replayFailingReplica) ReplicateReplayRecord(context.Context, sinfonia.ReplicateReplayRecordRequest) (sinfonia.ReplicateReplayRecordResponse, error) {
	return sinfonia.ReplicateReplayRecordResponse{Ack: false}, context.DeadlineExceeded
}

func TestPrimaryCopyBasicDurabilityWithPromotion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replica)

	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	prep, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 4, Data: []byte("R")}}})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Vote != sinfonia.VoteOk {
		t.Fatalf("expected OK prepare vote, got %v", prep.Vote)
	}
	if _, err := primary.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = replica.Close()
	promoted, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = promoted.Close() })
	promoted.PromoteToPrimary(0, nil)
	coordAfter := client.NewCoordinator(map[uint32]client.Participant{2: promoted})
	coordAfter.SetPrimaryMapping(map[uint32]uint32{10: 2})

	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		got, err := coordAfter.Read(ctx, 10, 4, 1)
		if err == nil && string(got) == "R" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected promoted replica to contain committed data, got=%q err=%v", string(got), err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestExactReplaySurvivesBaselineTransferAndPromotion(t *testing.T) {
	type replayCase struct {
		name  string
		setup func(context.Context, *memnode.Participant) (func(context.Context, *memnode.Participant) error, error)
	}
	cases := []replayCase{
		{
			name: "prepare-success",
			setup: func(ctx context.Context, primary *memnode.Participant) (func(context.Context, *memnode.Participant) error, error) {
				if err := primary.ApplyWriteForTest(0, []byte("seed")); err != nil {
					return nil, err
				}
				id := sinfonia.TransactionID{CoordinatorID: 201, Counter: 1, Epoch: 1}
				req := sinfonia.ExecPrepareRequest{TID: id, RequestID: "first", Participants: []uint32{1}, ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecAndPrepare(ctx, req)
				if err != nil || first.Vote != sinfonia.VoteOk || string(first.ReadData[0]) != "seed" {
					return nil, fmt.Errorf("initial prepare = %+v, err=%v", first, err)
				}
				if _, err := primary.Commit(ctx, sinfonia.CommitRequest{TID: id, ActionCommit: false}); err != nil {
					return nil, err
				}
				return func(ctx context.Context, promoted *memnode.Participant) error {
					if err := promoted.ApplyWriteForTest(0, []byte("next")); err != nil {
						return err
					}
					req.RequestID = "retry"
					req.WriteItems[0].Data = []byte("B")
					retry, err := promoted.ExecAndPrepare(ctx, req)
					if err != nil || retry.Vote != sinfonia.VoteOk || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
						return fmt.Errorf("replayed prepare = %+v, err=%v", retry, err)
					}
					got, err := promoted.Read(ctx, 8, 1)
					if err != nil {
						return err
					}
					if got[0] != 0 {
						return fmt.Errorf("replayed prepare applied write: %q", got)
					}
					return nil
				}, nil
			},
		},
		{
			name: "prepare-negative",
			setup: func(ctx context.Context, primary *memnode.Participant) (func(context.Context, *memnode.Participant) error, error) {
				id := sinfonia.TransactionID{CoordinatorID: 202, Counter: 1, Epoch: 1}
				req := sinfonia.ExecPrepareRequest{TID: id, RequestID: "first", Participants: []uint32{1}, CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecAndPrepare(ctx, req)
				if err != nil || first.Vote != sinfonia.VoteBadCmp {
					return nil, fmt.Errorf("initial prepare = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Participant) error {
					if err := promoted.ApplyWriteForTest(0, []byte("Y")); err != nil {
						return err
					}
					req.RequestID = "retry"
					retry, err := promoted.ExecAndPrepare(ctx, req)
					if err != nil || retry.Vote != sinfonia.VoteBadCmp || retry.RequestID != "retry" {
						return fmt.Errorf("replayed negative prepare = %+v, err=%v", retry, err)
					}
					got, err := promoted.Read(ctx, 8, 1)
					if err != nil {
						return err
					}
					if got[0] != 0 {
						return fmt.Errorf("replayed negative prepare applied write: %q", got)
					}
					return nil
				}, nil
			},
		},
		{
			name: "one-pc-success",
			setup: func(ctx context.Context, primary *memnode.Participant) (func(context.Context, *memnode.Participant) error, error) {
				if err := primary.ApplyWriteForTest(0, []byte("seed")); err != nil {
					return nil, err
				}
				id := sinfonia.TransactionID{CoordinatorID: 203, Counter: 1, Epoch: 1}
				req := sinfonia.ExecCommit1PCRequest{TID: id, RequestID: "first", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecCommit1PC(ctx, req)
				if err != nil || first.Vote != sinfonia.VoteOk || string(first.ReadData[0]) != "seed" {
					return nil, fmt.Errorf("initial 1pc = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Participant) error {
					if err := promoted.ApplyWriteForTest(0, []byte("next")); err != nil {
						return err
					}
					if err := promoted.ApplyWriteForTest(8, []byte("Z")); err != nil {
						return err
					}
					req.RequestID = "retry"
					req.WriteItems[0].Data = []byte("B")
					retry, err := promoted.ExecCommit1PC(ctx, req)
					if err != nil || retry.Vote != sinfonia.VoteOk || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
						return fmt.Errorf("replayed 1pc = %+v, err=%v", retry, err)
					}
					got, err := promoted.Read(ctx, 8, 1)
					if err != nil {
						return err
					}
					if string(got) != "Z" {
						return fmt.Errorf("replayed 1pc reapplied write: %q", got)
					}
					return nil
				}, nil
			},
		},
		{
			name: "one-pc-negative",
			setup: func(ctx context.Context, primary *memnode.Participant) (func(context.Context, *memnode.Participant) error, error) {
				id := sinfonia.TransactionID{CoordinatorID: 204, Counter: 1, Epoch: 1}
				req := sinfonia.ExecCommit1PCRequest{TID: id, RequestID: "first", CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecCommit1PC(ctx, req)
				if err != nil || first.Vote != sinfonia.VoteBadCmp {
					return nil, fmt.Errorf("initial 1pc = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Participant) error {
					if err := promoted.ApplyWriteForTest(0, []byte("Y")); err != nil {
						return err
					}
					req.RequestID = "retry"
					retry, err := promoted.ExecCommit1PC(ctx, req)
					if err != nil || retry.Vote != sinfonia.VoteBadCmp || retry.RequestID != "retry" {
						return fmt.Errorf("replayed negative 1pc = %+v, err=%v", retry, err)
					}
					got, err := promoted.Read(ctx, 8, 1)
					if err != nil {
						return err
					}
					if got[0] != 0 {
						return fmt.Errorf("replayed negative 1pc applied write: %q", got)
					}
					return nil
				}, nil
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = primary.Close() })
			replica, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "replica"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = replica.Close() })
			if err := primary.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if err := replica.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			retry, err := testCase.setup(ctx, primary)
			if err != nil {
				t.Fatal(err)
			}
			replica.ConfigureAsReplica(1)
			primary.ConfigureAsPrimary(2, replica)
			if err := replica.SyncFromPrimary(ctx, primary); err != nil {
				t.Fatal(err)
			}
			replica.PromoteToPrimary(0, nil)
			if err := retry(ctx, replica); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExactReplayLiveReplicationSurvivesPromotion(t *testing.T) {
	t.Run("negative-prepare", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = primary.Close() })
		replica, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "replica"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = replica.Close() })
		if err := primary.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if err := replica.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		replica.ConfigureAsReplica(1)
		primary.ConfigureAsPrimary(2, replica)
		txId := sinfonia.TransactionID{CoordinatorID: 205, Counter: 1, Epoch: 1}
		req := sinfonia.ExecPrepareRequest{TID: txId, RequestID: "first", Participants: []uint32{1}, CmpItems: []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
		first, err := primary.ExecAndPrepare(ctx, req)
		if err != nil || first.Vote != sinfonia.VoteBadCmp {
			t.Fatalf("initial prepare = %+v, err=%v", first, err)
		}
		if err := replica.ApplyWriteForTest(0, []byte("Y")); err != nil {
			t.Fatal(err)
		}
		replica.PromoteToPrimary(0, nil)
		req.RequestID = "retry"
		retry, err := replica.ExecAndPrepare(ctx, req)
		if err != nil || retry.Vote != sinfonia.VoteBadCmp || retry.RequestID != "retry" {
			t.Fatalf("replayed prepare = %+v, err=%v", retry, err)
		}
		got, err := replica.Read(ctx, 8, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] != 0 {
			t.Fatalf("replayed negative prepare applied write: %q", got)
		}
	})

	t.Run("one-pc-success", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = primary.Close() })
		replica, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "replica"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = replica.Close() })
		if err := primary.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if err := replica.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if err := primary.ApplyWriteForTest(0, []byte("seed")); err != nil {
			t.Fatal(err)
		}
		if err := replica.ApplyWriteForTest(0, []byte("seed")); err != nil {
			t.Fatal(err)
		}
		replica.ConfigureAsReplica(1)
		primary.ConfigureAsPrimary(2, replica)
		id := sinfonia.TransactionID{CoordinatorID: 206, Counter: 1, Epoch: 1}
		req := sinfonia.ExecCommit1PCRequest{TID: id, RequestID: "first", ReadItems: []sinfonia.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
		first, err := primary.ExecCommit1PC(ctx, req)
		if err != nil || first.Vote != sinfonia.VoteOk || string(first.ReadData[0]) != "seed" {
			t.Fatalf("initial 1pc = %+v, err=%v", first, err)
		}
		if err := replica.ApplyWriteForTest(0, []byte("next")); err != nil {
			t.Fatal(err)
		}
		if err := replica.ApplyWriteForTest(8, []byte("Z")); err != nil {
			t.Fatal(err)
		}
		replica.PromoteToPrimary(0, nil)
		req.RequestID = "retry"
		req.WriteItems[0].Data = []byte("B")
		retry, err := replica.ExecCommit1PC(ctx, req)
		if err != nil || retry.Vote != sinfonia.VoteOk || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
			t.Fatalf("replayed 1pc = %+v, err=%v", retry, err)
		}
		got, err := replica.Read(ctx, 8, 1)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "Z" {
			t.Fatalf("replayed 1pc reapplied write: %q", got)
		}
	})
}

func TestReplicationBlockingPrimaryWaitsForReplicaAck(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	replica, _ := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	t.Cleanup(func() { _ = primary.Close() })
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, &slowReplica{node: replica, delay: 80 * time.Millisecond})

	txId := sinfonia.TransactionID{CoordinatorID: 3, Counter: 1, Epoch: 1}
	start := time.Now()
	resp, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != sinfonia.VoteOk {
		t.Fatalf("expected OK vote, got %v", resp.Vote)
	}
	if time.Since(start) < 80*time.Millisecond {
		t.Fatalf("expected prepare to block for replica acknowledgement")
	}
}

func TestReplicaCrashDuringPrepareVotesBadInternal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	t.Cleanup(func() { _ = primary.Close() })
	primary.ConfigureAsPrimary(2, failingReplica{})

	txId := sinfonia.TransactionID{CoordinatorID: 8, Counter: 1, Epoch: 1}
	resp, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("q")}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("expected BAD_INTERNAL vote, got %v", resp.Vote)
	}
}

func TestReplayReplicationFailureIsRepairedOnRetry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replayFailingReplica{node: replica})

	txId := sinfonia.TransactionID{CoordinatorID: 207, Counter: 1, Epoch: 1}
	req := sinfonia.ExecPrepareRequest{
		TID:          txId,
		RequestID:    "first",
		Participants: []uint32{1},
		CmpItems:     []sinfonia.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}},
		WriteItems:   []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	first, err := primary.ExecAndPrepare(ctx, req)
	if err == nil || first.Vote != sinfonia.VoteBadInternal {
		t.Fatalf("initial failed replication = %+v, err=%v", first, err)
	}

	primary.ConfigureAsPrimary(2, replica)
	req.RequestID = "repair"
	repaired, err := primary.ExecAndPrepare(ctx, req)
	if err != nil || repaired.Vote != sinfonia.VoteBadCmp || repaired.RequestID != req.RequestID {
		t.Fatalf("repaired retry = %+v, err=%v", repaired, err)
	}

	replica.PromoteToPrimary(0, nil)
	req.RequestID = "promoted"
	replayed, err := replica.ExecAndPrepare(ctx, req)
	if err != nil || replayed.Vote != sinfonia.VoteBadCmp || replayed.RequestID != req.RequestID {
		t.Fatalf("promoted retry = %+v, err=%v", replayed, err)
	}
	got, err := replica.Read(ctx, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0 {
		t.Fatalf("repaired replay applied a write: %q", got)
	}
}

func TestCommitDecisionReplicationAndReplicaRecovery(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, _ := memnode.NewDurableParticipant(1, 64, filepath.Join(root, "primary"))
	replica, _ := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	t.Cleanup(func() { _ = primary.Close() })
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replica)

	txId := sinfonia.TransactionID{CoordinatorID: 5, Counter: 2, Epoch: 1}
	prepare, err := primary.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 9, Data: []byte("m")}}})
	if err != nil {
		t.Fatal(err)
	}
	if prepare.Vote != sinfonia.VoteOk {
		t.Fatalf("expected OK vote, got %v", prepare.Vote)
	}
	if _, err := primary.Commit(ctx, sinfonia.CommitRequest{TID: txId, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = replica.Close()
	replicaRestarted, err := memnode.NewDurableParticipant(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replicaRestarted.Close() })
	replicaRestarted.ConfigureAsReplica(1)
	if err := replicaRestarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := replicaRestarted.Read(ctx, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "m" {
		t.Fatalf("expected replica recovery state to match committed primary write, got %q", string(got))
	}
}
