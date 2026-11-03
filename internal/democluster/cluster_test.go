package democluster

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestProcessesRecoverPreparedTransactionThroughPeerRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	executable := os.Getenv("MEMNODE_BIN")
	dir := t.TempDir()
	cluster, err := Start(ctx, executable, dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close() // A forced exit below is intentional.
	tid := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	for id := uint32(1); id <= 2; id++ {
		peer := rpcwire.NewClient(cluster.config[id].RPCAddress, rpcwire.ClientOptions{})
		request := sinfonia.ExecPrepareRequest{
			TID: tid, RequestID: sinfonia.RequestID(fmt.Sprintf("prepare-%d", id)), Epoch: tid.Epoch,
			Participants: []uint32{1, 2}, HasWrites: true,
			WriteItems: []sinfonia.WriteItem{{MemID: id, Addr: 0, Data: []byte{byte(id)}}},
		}
		var vote sinfonia.ExecPrepareResponse
		callErr := peer.Call(ctx, sinfonia.MethodExecPrepare, request, &vote)
		_ = peer.Close()
		if callErr != nil || vote.Vote != sinfonia.VoteOk {
			t.Fatalf("node %d prepare: vote=%v error=%v", id, vote.Vote, callErr)
		}
	}
	// Lose both processes after their yes votes, before any commit decision RPC.
	for _, node := range cluster.nodes {
		if err := node.command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range cluster.nodes {
		select {
		case <-node.done:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	reopened, err := Start(ctx, executable, dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for id := uint32(1); id <= 2; id++ {
		got, err := reopened.Client.Read(ctx, id, 0, 1)
		if err != nil || len(got) != 1 || got[0] != byte(id) {
			t.Errorf("node %d recovered %x, error=%v", id, got, err)
		}
	}
}

func TestCancellationStopsChildProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	executable := os.Getenv("MEMNODE_BIN")
	childCtx, stop := context.WithCancel(ctx)
	defer stop()
	cluster, err := Start(childCtx, executable, t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	stop()
	for _, node := range cluster.nodes {
		select {
		case <-node.done:
			if node.err != nil {
				t.Error(node.err)
			}
		case <-ctx.Done():
			t.Fatal("child process survived cancellation")
		}
	}
}

func TestProcessesRecoverAbandonedCoordinatorWithoutRestart(t *testing.T) {
	for _, allPrepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("all-prepared=%t", allPrepared), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			dir := t.TempDir()
			cluster, err := Start(ctx, os.Getenv("MEMNODE_BIN"), dir, 64)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			tid := sinfonia.TransactionID{CoordinatorID: 44, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
			count := uint32(1)
			if allPrepared {
				count = 2
			}
			for id := uint32(1); id <= count; id++ {
				peer := rpcwire.NewClient(cluster.config[id].RPCAddress, rpcwire.ClientOptions{})
				var vote sinfonia.ExecPrepareResponse
				err := peer.Call(ctx, sinfonia.MethodExecPrepare, sinfonia.ExecPrepareRequest{TID: tid, Epoch: tid.Epoch, Participants: []uint32{1, 2}, HasWrites: true, WriteItems: []sinfonia.WriteItem{{MemID: id, Data: []byte{byte(id)}}}}, &vote)
				peer.Close()
				if err != nil || vote.Vote != sinfonia.VoteOk {
					t.Fatalf("prepare node %d: %+v, %v", id, vote, err)
				}
			}
			// No client sends a decision. The nodes must resolve the held range locks.
			for id := uint32(1); id <= 2; id++ {
				want := byte(0)
				if allPrepared {
					want = byte(id)
				}
				for {
					got, err := cluster.Client.Read(ctx, id, 0, 1)
					if err == nil && len(got) == 1 && got[0] == want {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatalf("node %d stayed blocked: %x, %v", id, got, err)
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			if !allPrepared {
				peer := rpcwire.NewClient(cluster.config[2].RPCAddress, rpcwire.ClientOptions{})
				defer peer.Close()
				var vote sinfonia.ExecPrepareResponse
				err := peer.Call(ctx, sinfonia.MethodExecPrepare, sinfonia.ExecPrepareRequest{TID: tid, Epoch: tid.Epoch, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 2, Data: []byte("late")}}}, &vote)
				if err != nil || vote.Vote != sinfonia.VoteBadForced {
					t.Fatalf("late prepare reversed recovery: %+v, %v", vote, err)
				}
			}
		})
	}
}
