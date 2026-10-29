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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	executable := os.Getenv("MEMNODE_BIN")
	childCtx, stop := context.WithCancel(ctx)
	defer stop()
	cluster, err := Start(childCtx, executable, "", 64)
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
