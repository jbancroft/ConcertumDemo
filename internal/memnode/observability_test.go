package memnode

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestMetricsCommitAbortAndRetry(t *testing.T) {
	n := NewParticipant(32)
	tid1 := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	_, err := n.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: tid1, Participants: []uint32{1}, Class: "payments", WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("a")}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = n.Commit(context.Background(), sinfonia.CommitRequest{TID: tid1, ActionCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	tid2 := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	_, err = n.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: tid2, Participants: []uint32{1}, Class: "payments", WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 1, Data: []byte("b")}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = n.Commit(context.Background(), sinfonia.CommitRequest{TID: tid2, ActionCommit: false})
	if err != nil {
		t.Fatal(err)
	}
	n.RecordRetryForClass("payments")

	if n.metrics.minitransactionsCommitted.Value() == 0 {
		t.Fatalf("expected committed counter increment")
	}
	if n.metrics.minitransactionsRetried.Value() == 0 {
		t.Fatalf("expected retry counter increment")
	}
	if n.metrics.minitransactionsAborted.Value() == 0 {
		t.Fatalf("expected abort counter increment")
	}
	snap := n.StatsSnapshot()
	p, ok := snap.ByClass["payments"]
	if !ok {
		t.Fatalf("expected by_class payments entry")
	}
	if p.Total != 2 || p.Committed != 1 || p.Aborted != 1 || p.Retried != 1 {
		t.Fatalf("unexpected payments stats: %+v", p)
	}
	if p.Rates.ExecutedPerSec.Medium <= 0 || p.Rates.RetriedPerSec.Medium <= 0 || p.Rates.AbortedPerSec.Medium <= 0 {
		t.Fatalf("expected non-zero class rates, got %+v", p.Rates)
	}
}

func TestStatsSnapshotRateWindows(t *testing.T) {
	n := NewParticipant(32)
	base := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	now := base
	n.nowFn = func() time.Time { return now }
	recordAt := func(ts time.Time, class string, committed bool) {
		n.metrics.ObserveClassDecision(class, committed, ts)
	}
	recordRetryAt := func(ts time.Time, class string) {
		n.metrics.ObserveClassRetry(class, ts)
	}

	// Executed at ~5s, 30s, and 200s ago; one abort at 30s.
	recordAt(base.Add(-5*time.Second), "ops", true)
	recordAt(base.Add(-30*time.Second), "ops", false)
	recordAt(base.Add(-200*time.Second), "ops", true)
	// Retries at ~8s and 100s ago.
	recordRetryAt(base.Add(-8*time.Second), "ops")
	recordRetryAt(base.Add(-100*time.Second), "ops")
	// Bytes at ~2s, 20s, and 4m ago.
	n.metrics.bytesEvents.Add(base.Add(-2*time.Second), 100)
	n.metrics.bytesEvents.Add(base.Add(-20*time.Second), 100)
	n.metrics.bytesEvents.Add(base.Add(-4*time.Minute), 100)

	snap := n.StatsSnapshot()
	if snap.Rates.Txns.ExecutedPerSec.Short != 0.1 {
		t.Fatalf("expected short executed rate 0.1, got %v", snap.Rates.Txns.ExecutedPerSec.Short)
	}
	if snap.Rates.Txns.ExecutedPerSec.Medium != 0.03 {
		t.Fatalf("expected medium executed rate 0.03, got %v", snap.Rates.Txns.ExecutedPerSec.Medium)
	}
	if snap.Rates.Txns.ExecutedPerSec.Long != 0.01 {
		t.Fatalf("expected long executed rate 0.01, got %v", snap.Rates.Txns.ExecutedPerSec.Long)
	}
	if snap.Rates.Txns.RetriedPerSec.Short != 0.1 || snap.Rates.Txns.RetriedPerSec.Medium != 0.02 || snap.Rates.Txns.RetriedPerSec.Long != 0.01 {
		t.Fatalf("unexpected retried rates: %+v", snap.Rates.Txns.RetriedPerSec)
	}
	if snap.Rates.Txns.AbortedPerSec.Short != 0 || snap.Rates.Txns.AbortedPerSec.Medium != 0.02 || snap.Rates.Txns.AbortedPerSec.Long != 0 {
		t.Fatalf("unexpected aborted rates: %+v", snap.Rates.Txns.AbortedPerSec)
	}
	if snap.Rates.BytesWrittenPerSec.Short != 10 || snap.Rates.BytesWrittenPerSec.Medium != 3.33 || snap.Rates.BytesWrittenPerSec.Long != 1 {
		t.Fatalf("unexpected bytes rates: %+v", snap.Rates.BytesWrittenPerSec)
	}
}

func TestBadLockIncludesConflictContext(t *testing.T) {
	n := NewParticipant(64)
	ctx := context.Background()
	tid1 := sinfonia.TransactionID{CoordinatorID: 7, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	tid2 := sinfonia.TransactionID{CoordinatorID: 7, Counter: 2, Epoch: sinfonia.EpochAt(time.Now())}
	if resp, err := n.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: tid1, CorrelationID: tid1.CorrelationID(), Participants: []uint32{1},
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("abc")}},
	}); err != nil || resp.Vote != sinfonia.VoteOk {
		t.Fatalf("first prepare vote=%v err=%v", resp.Vote, err)
	}
	resp, err := n.ExecAndPrepare(ctx, sinfonia.ExecPrepareRequest{
		TID: tid2, CorrelationID: tid2.CorrelationID(), Participants: []uint32{1},
		WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 8, Data: []byte("z")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Vote != sinfonia.VoteBadLock || resp.LockConflict == nil {
		t.Fatalf("expected bad lock with conflict details: %+v", resp)
	}
	if resp.LockConflict.HeldStart != 8 {
		t.Fatalf("unexpected held range %+v", resp.LockConflict)
	}
}

func TestClassAttributionFallbackAndCardinalityCap(t *testing.T) {
	n := NewParticipant(16)
	for i := 0; i < maxTxnClasses+10; i++ {
		class := "class-" + strconv.Itoa(i)
		n.metrics.ObserveClassDecision(class, true, time.Now())
	}
	n.metrics.ObserveClassDecision("", false, time.Now())

	snap := n.StatsSnapshot()
	if len(snap.ByClass) != maxTxnClasses {
		t.Fatalf("expected class map capped at %d, got %d", maxTxnClasses, len(snap.ByClass))
	}
	unclassified, ok := snap.ByClass[unclassifiedTxnClass]
	if !ok {
		t.Fatalf("expected fallback bucket %q", unclassifiedTxnClass)
	}
	if unclassified.Total == 0 || unclassified.Aborted == 0 {
		t.Fatalf("expected overflow and blank classes to roll into fallback bucket, got %+v", unclassified)
	}
}

func TestFsyncHistogramAndReplicationLag(t *testing.T) {
	dir := t.TempDir()
	primary, err := NewDurableParticipant(1, 64, filepath.Join(dir, "p"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	replica, err := NewDurableParticipant(2, 64, filepath.Join(dir, "r"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	replica.ConfigureAsReplica(1)

	primary.ConfigureAsPrimary(2, replica)
	txId := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: sinfonia.EpochAt(time.Now())}
	_, err = primary.ExecAndPrepare(context.Background(), sinfonia.ExecPrepareRequest{TID: txId, Participants: []uint32{1, 2}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: 0, Data: []byte("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Commit(context.Background(), sinfonia.CommitRequest{TID: txId, ActionCommit: true}); err != nil {
		t.Fatal(err)
	}
	if primary.metrics.fsyncLatency.total.Load() == 0 {
		t.Fatalf("expected fsync latency histogram record")
	}
	if replica.metrics.replicaLagEntries.Value() == 0 {
		t.Fatalf("expected replica lag gauge >0 before sync")
	}
	if err := replica.SyncFromPrimary(context.Background(), primary); err != nil {
		t.Fatal(err)
	}
	if replica.metrics.replicaLagEntries.Value() != 0 {
		t.Fatalf("expected replica lag gauge to clear after sync")
	}
}
