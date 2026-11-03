package memnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

func TestRPCExecutionLimitSharedAcrossPrepareAndOnePhase(t *testing.T) {
	n := newTestParticipant(t, 1, 16)
	client := startAdmissionTestRPC(t, n)
	ctx := context.Background()
	epoch := sinfonia.EpochAt(time.Now())
	tid := func(counter int) sinfonia.TransactionID {
		return sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(counter), Epoch: epoch}
	}

	// Hold readiness so accepted RPCs remain in flight before executing.
	n.mu.Lock()
	unblock := sync.OnceFunc(n.mu.Unlock)
	var calls sync.WaitGroup
	defer func() { unblock(); calls.Wait() }()
	results := make(chan error, maxConcurrentExecutions)
	for i := 0; i < maxConcurrentExecutions; i++ {
		calls.Add(1)
		go func(i int) {
			defer calls.Done()
			results <- callAdmissionTestRPC(ctx, client, i%2 == 0, tid(i+1))
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	// Observe admission only to synchronize the requests; assertions below
	// check the RPC behavior at capacity and after the accepted work finishes.
	for len(n.executionSlots) < maxConcurrentExecutions {
		if time.Now().After(deadline) {
			t.Fatalf("only %d execution requests arrived", len(n.executionSlots))
		}
		time.Sleep(time.Millisecond)
	}
	for _, onePhase := range []bool{false, true} {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		err := callAdmissionTestRPC(probe, client, onePhase, tid(maxConcurrentExecutions+1))
		cancel()
		var remote *rpcwire.RemoteError
		if !errors.As(err, &remote) || remote.Message != ErrResourceExhausted.Error() {
			t.Fatalf("onePhase=%t: excess execution = %v, want resource exhaustion", onePhase, err)
		}
	}

	unblock()
	calls.Wait()
	for i := 0; i < maxConcurrentExecutions; i++ {
		if err := <-results; err != nil {
			t.Fatalf("accepted execution failed: %v", err)
		}
	}
	for i, onePhase := range []bool{false, true} {
		if err := callAdmissionTestRPC(ctx, client, onePhase, tid(maxConcurrentExecutions+1+i)); err != nil {
			t.Fatalf("execution after capacity returned: %v", err)
		}
	}
}

func TestRPCExecutionLimitReleasesRejectedRequests(t *testing.T) {
	n := newTestParticipant(t, 1, 16)
	n.recoveryComplete = false
	client := startAdmissionTestRPC(t, n)
	ctx := context.Background()
	tid := sinfonia.TransactionID{CoordinatorID: 1, Epoch: sinfonia.EpochAt(time.Now())}
	for i := 0; i <= 2*maxConcurrentExecutions; i++ {
		err := callAdmissionTestRPC(ctx, client, i%2 == 0, tid)
		var remote *rpcwire.RemoteError
		if !errors.As(err, &remote) || remote.Message != ErrRecoveryPending.Error() {
			t.Fatalf("request %d: %v, want recovery pending", i, err)
		}
	}
	n.mu.Lock()
	n.recoveryComplete = true
	n.mu.Unlock()
	if err := callAdmissionTestRPC(ctx, client, true, tid); err != nil {
		t.Fatalf("execution after recovery: %v", err)
	}
}

func startAdmissionTestRPC(t *testing.T, n *Participant) *rpcwire.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- n.RPCServer().Serve(listener) }()
	client := rpcwire.NewClient(listener.Addr().String(), rpcwire.ClientOptions{CallTimeout: 10 * time.Second})
	t.Cleanup(func() {
		_ = client.Close()
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
		_ = n.Close()
	})
	return client
}

func callAdmissionTestRPC(ctx context.Context, client *rpcwire.Client, onePhase bool, tid sinfonia.TransactionID) error {
	var vote sinfonia.Vote
	var err error
	if onePhase {
		var resp sinfonia.ExecCommit1PCResponse
		err = client.Call(ctx, sinfonia.MethodExecCommit1PC, sinfonia.ExecCommit1PCRequest{TID: tid}, &resp)
		vote = resp.Vote
	} else {
		var resp sinfonia.ExecPrepareResponse
		err = client.Call(ctx, sinfonia.MethodExecPrepare, sinfonia.ExecPrepareRequest{TID: tid, Participants: []uint32{1}}, &resp)
		vote = resp.Vote
	}
	if err == nil && vote != sinfonia.VoteOk {
		return fmt.Errorf("execution vote = %v, want OK", vote)
	}
	return err
}
