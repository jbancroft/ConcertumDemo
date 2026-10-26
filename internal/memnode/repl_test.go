package memnode_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/client"
	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
)

type slowReplica struct {
	node  *memnode.Node
	delay time.Duration
}

func (s *slowReplica) ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateRedoEntry(ctx, req)
}

func (s *slowReplica) ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateCommitDecision(ctx, req)
}

func (s *slowReplica) ReplicateReplayRecord(ctx context.Context, req proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	time.Sleep(s.delay)
	return s.node.ReplicateReplayRecord(ctx, req)
}

type failingReplica struct{}

func (f failingReplica) ReplicateRedoEntry(context.Context, proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	return proto.ReplicateRedoEntryResponse{Ack: false}, context.DeadlineExceeded
}

func (f failingReplica) ReplicateCommitDecision(context.Context, proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	return proto.ReplicateCommitDecisionResponse{Ack: false}, context.DeadlineExceeded
}

func (f failingReplica) ReplicateReplayRecord(context.Context, proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	return proto.ReplicateReplayRecordResponse{Ack: false}, context.DeadlineExceeded
}

type replayFailingReplica struct {
	node *memnode.Node
}

func (r replayFailingReplica) ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	return r.node.ReplicateRedoEntry(ctx, req)
}

func (r replayFailingReplica) ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	return r.node.ReplicateCommitDecision(ctx, req)
}

func (replayFailingReplica) ReplicateReplayRecord(context.Context, proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	return proto.ReplicateReplayRecordResponse{Ack: false}, context.DeadlineExceeded
}

func TestPrimaryCopyBasicDurabilityWithPromotion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replica)

	tid := proto.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 1}
	prep, err := primary.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 4, Data: []byte("R")}}})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Vote != proto.VoteOK {
		t.Fatalf("expected OK prepare vote, got %v", prep.Vote)
	}
	if _, err := primary.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = replica.Close()
	promoted, err := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = promoted.Close() })
	promoted.PromoteToPrimary(0, nil)
	coordAfter := client.NewCoordinator(map[uint32]client.MemNode{2: promoted})
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
		setup func(context.Context, *memnode.Node) (func(context.Context, *memnode.Node) error, error)
	}
	cases := []replayCase{
		{
			name: "prepare-success",
			setup: func(ctx context.Context, primary *memnode.Node) (func(context.Context, *memnode.Node) error, error) {
				if err := primary.ApplyWriteForTest(0, []byte("seed")); err != nil {
					return nil, err
				}
				id := proto.TransactionID{CoordinatorID: 201, Counter: 1, Epoch: 1}
				req := proto.ExecPrepareRequest{TID: id, RequestID: "first", Participants: []uint32{1}, ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecPrepare(ctx, req)
				if err != nil || first.Vote != proto.VoteOK || string(first.ReadData[0]) != "seed" {
					return nil, fmt.Errorf("initial prepare = %+v, err=%v", first, err)
				}
				if _, err := primary.Commit(ctx, proto.CommitRequest{TID: id, ActionCommit: false}); err != nil {
					return nil, err
				}
				return func(ctx context.Context, promoted *memnode.Node) error {
					if err := promoted.ApplyWriteForTest(0, []byte("next")); err != nil {
						return err
					}
					req.RequestID = "retry"
					req.WriteItems[0].Data = []byte("B")
					retry, err := promoted.ExecPrepare(ctx, req)
					if err != nil || retry.Vote != proto.VoteOK || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
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
			setup: func(ctx context.Context, primary *memnode.Node) (func(context.Context, *memnode.Node) error, error) {
				id := proto.TransactionID{CoordinatorID: 202, Counter: 1, Epoch: 1}
				req := proto.ExecPrepareRequest{TID: id, RequestID: "first", Participants: []uint32{1}, CmpItems: []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecPrepare(ctx, req)
				if err != nil || first.Vote != proto.VoteBadCmp {
					return nil, fmt.Errorf("initial prepare = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Node) error {
					if err := promoted.ApplyWriteForTest(0, []byte("Y")); err != nil {
						return err
					}
					req.RequestID = "retry"
					retry, err := promoted.ExecPrepare(ctx, req)
					if err != nil || retry.Vote != proto.VoteBadCmp || retry.RequestID != "retry" {
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
			setup: func(ctx context.Context, primary *memnode.Node) (func(context.Context, *memnode.Node) error, error) {
				if err := primary.ApplyWriteForTest(0, []byte("seed")); err != nil {
					return nil, err
				}
				id := proto.TransactionID{CoordinatorID: 203, Counter: 1, Epoch: 1}
				req := proto.ExecCommit1PCRequest{TID: id, RequestID: "first", ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecCommit1PC(ctx, req)
				if err != nil || first.Vote != proto.VoteOK || string(first.ReadData[0]) != "seed" {
					return nil, fmt.Errorf("initial 1pc = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Node) error {
					if err := promoted.ApplyWriteForTest(0, []byte("next")); err != nil {
						return err
					}
					if err := promoted.ApplyWriteForTest(8, []byte("Z")); err != nil {
						return err
					}
					req.RequestID = "retry"
					req.WriteItems[0].Data = []byte("B")
					retry, err := promoted.ExecCommit1PC(ctx, req)
					if err != nil || retry.Vote != proto.VoteOK || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
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
			setup: func(ctx context.Context, primary *memnode.Node) (func(context.Context, *memnode.Node) error, error) {
				id := proto.TransactionID{CoordinatorID: 204, Counter: 1, Epoch: 1}
				req := proto.ExecCommit1PCRequest{TID: id, RequestID: "first", CmpItems: []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
				first, err := primary.ExecCommit1PC(ctx, req)
				if err != nil || first.Vote != proto.VoteBadCmp {
					return nil, fmt.Errorf("initial 1pc = %+v, err=%v", first, err)
				}
				return func(ctx context.Context, promoted *memnode.Node) error {
					if err := promoted.ApplyWriteForTest(0, []byte("Y")); err != nil {
						return err
					}
					req.RequestID = "retry"
					retry, err := promoted.ExecCommit1PC(ctx, req)
					if err != nil || retry.Vote != proto.VoteBadCmp || retry.RequestID != "retry" {
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
			primary, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = primary.Close() })
			replica, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "replica"))
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
		primary, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = primary.Close() })
		replica, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "replica"))
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
		tid := proto.TransactionID{CoordinatorID: 205, Counter: 1, Epoch: 1}
		req := proto.ExecPrepareRequest{TID: tid, RequestID: "first", Participants: []uint32{1}, CmpItems: []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
		first, err := primary.ExecPrepare(ctx, req)
		if err != nil || first.Vote != proto.VoteBadCmp {
			t.Fatalf("initial prepare = %+v, err=%v", first, err)
		}
		if err := replica.ApplyWriteForTest(0, []byte("Y")); err != nil {
			t.Fatal(err)
		}
		replica.PromoteToPrimary(0, nil)
		req.RequestID = "retry"
		retry, err := replica.ExecPrepare(ctx, req)
		if err != nil || retry.Vote != proto.VoteBadCmp || retry.RequestID != "retry" {
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
		primary, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = primary.Close() })
		replica, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "replica"))
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
		id := proto.TransactionID{CoordinatorID: 206, Counter: 1, Epoch: 1}
		req := proto.ExecCommit1PCRequest{TID: id, RequestID: "first", ReadItems: []proto.ReadItem{{MemID: 1, Addr: 0, Len: 4}}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}}}
		first, err := primary.ExecCommit1PC(ctx, req)
		if err != nil || first.Vote != proto.VoteOK || string(first.ReadData[0]) != "seed" {
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
		if err != nil || retry.Vote != proto.VoteOK || retry.RequestID != "retry" || string(retry.ReadData[0]) != "seed" {
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
	primary, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
	replica, _ := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
	t.Cleanup(func() { _ = primary.Close() })
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, &slowReplica{node: replica, delay: 80 * time.Millisecond})

	tid := proto.TransactionID{CoordinatorID: 3, Counter: 1, Epoch: 1}
	start := time.Now()
	resp, err := primary.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 0, Data: []byte("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != proto.VoteOK {
		t.Fatalf("expected OK vote, got %v", resp.Vote)
	}
	if time.Since(start) < 80*time.Millisecond {
		t.Fatalf("expected prepare to block for replica acknowledgement")
	}
}

func TestReplicaCrashDuringPrepareVotesBadInternal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
	t.Cleanup(func() { _ = primary.Close() })
	primary.ConfigureAsPrimary(2, failingReplica{})

	tid := proto.TransactionID{CoordinatorID: 8, Counter: 1, Epoch: 1}
	resp, err := primary.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 1, Data: []byte("q")}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != proto.VoteBadInternal {
		t.Fatalf("expected BAD_INTERNAL vote, got %v", resp.Vote)
	}
}

func TestReplayReplicationFailureIsRepairedOnRetry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primary, err := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replayFailingReplica{node: replica})

	tid := proto.TransactionID{CoordinatorID: 207, Counter: 1, Epoch: 1}
	req := proto.ExecPrepareRequest{
		TID:          tid,
		RequestID:    "first",
		Participants: []uint32{1},
		CmpItems:     []proto.CmpItem{{MemID: 1, Addr: 0, Data: []byte("Y")}},
		WriteItems:   []proto.WriteItem{{MemID: 1, Addr: 8, Data: []byte("A")}},
	}
	first, err := primary.ExecPrepare(ctx, req)
	if err == nil || first.Vote != proto.VoteBadInternal {
		t.Fatalf("initial failed replication = %+v, err=%v", first, err)
	}

	primary.ConfigureAsPrimary(2, replica)
	req.RequestID = "repair"
	repaired, err := primary.ExecPrepare(ctx, req)
	if err != nil || repaired.Vote != proto.VoteBadCmp || repaired.RequestID != req.RequestID {
		t.Fatalf("repaired retry = %+v, err=%v", repaired, err)
	}

	replica.PromoteToPrimary(0, nil)
	req.RequestID = "promoted"
	replayed, err := replica.ExecPrepare(ctx, req)
	if err != nil || replayed.Vote != proto.VoteBadCmp || replayed.RequestID != req.RequestID {
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
	primary, _ := memnode.NewDurableNode(1, 64, filepath.Join(root, "primary"))
	replica, _ := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
	t.Cleanup(func() { _ = primary.Close() })
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)
	primary.ConfigureAsPrimary(2, replica)

	tid := proto.TransactionID{CoordinatorID: 5, Counter: 2, Epoch: 1}
	prepare, err := primary.ExecPrepare(ctx, proto.ExecPrepareRequest{TID: tid, Participants: []uint32{1, 2}, WriteItems: []proto.WriteItem{{MemID: 1, Addr: 9, Data: []byte("m")}}})
	if err != nil {
		t.Fatal(err)
	}
	if prepare.Vote != proto.VoteOK {
		t.Fatalf("expected OK vote, got %v", prepare.Vote)
	}
	if _, err := primary.Commit(ctx, proto.CommitRequest{TID: tid, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}

	_ = replica.Close()
	replicaRestarted, err := memnode.NewDurableNode(2, 64, filepath.Join(root, "replica"))
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
