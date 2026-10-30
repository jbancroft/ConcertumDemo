package memnode

import (
	"context"
	"fmt"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const unclassifiedTxnClass = "__unclassified__"
const maxTxnClasses = 128
const (
	rateWindowShort    = 10 * time.Second
	rateWindowMedium   = time.Minute
	rateWindowLong     = 5 * time.Minute
	maxRateEvents      = 20_000
	retainedRateEvents = 10_000
)

type counter struct{ v atomic.Uint64 }

func (c *counter) Inc()          { c.v.Add(1) }
func (c *counter) Add(n uint64)  { c.v.Add(n) }
func (c *counter) Value() uint64 { return c.v.Load() }

type gauge struct{ v atomic.Int64 }

func (g *gauge) Set(n int64)  { g.v.Store(n) }
func (g *gauge) Value() int64 { return g.v.Load() }

type histogram struct {
	buckets  []time.Duration
	counts   []atomic.Uint64
	sumNanos atomic.Uint64
	total    atomic.Uint64
}

func newHistogram(bounds []time.Duration) *histogram {
	return &histogram{buckets: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.total.Add(1)
	h.sumNanos.Add(uint64(d.Nanoseconds()))
	idx := len(h.buckets)
	for i, b := range h.buckets {
		if d <= b {
			idx = i
			break
		}
	}
	h.counts[idx].Add(1)
}

type rollingWindow struct {
	mu     sync.Mutex
	events []timedValue
}

type timedValue struct {
	ts    time.Time
	value float64
}

func (r *rollingWindow) Add(ts time.Time, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, timedValue{ts: ts, value: v})
	if len(r.events) > maxRateEvents {
		r.events = append([]timedValue(nil), r.events[len(r.events)-retainedRateEvents:]...)
	}
}

func (r *rollingWindow) Rate(window time.Duration, now time.Time) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if window <= 0 {
		return 0
	}
	cut := now.Add(-window)
	sum := 0.0
	for _, e := range r.events {
		if e.ts.After(cut) {
			sum += e.value
		}
	}
	return sum / window.Seconds()
}

type nodeMetrics struct {
	minitransactionsTotal     counter
	minitransactionsCommitted counter
	minitransactionsAborted   counter
	minitransactionsRetried   counter
	redoEntriesWritten        counter
	redoFsyncs                counter
	replicationEntriesSent    counter
	replicationFailures       counter
	recoveryRuns              counter
	forcedAbortCount          counter
	logGCRuns                 counter
	backupRuns                counter
	rejectedRequestsTotal     counter
	rateLimitedTotal          counter
	requestSizeRejectedTotal  counter

	inDoubtCount            gauge
	forcedAbortCountCurrent gauge
	redoLogSizeBytes        gauge
	diskImageSizeBytes      gauge
	pendingNotifications    gauge
	replicaLagEntries       gauge

	prepareLatency              *histogram
	commitLatency               *histogram
	endToEndTxnLatency          *histogram
	fsyncLatency                *histogram
	replicationRoundtripLatency *histogram

	executedEvents rollingWindow
	retriedEvents  rollingWindow
	abortedEvents  rollingWindow
	bytesEvents    rollingWindow

	classMu    sync.Mutex
	classStats map[string]*txnClassStats
}

type txnClassStats struct {
	total          counter
	committed      counter
	aborted        counter
	retried        counter
	executedEvents rollingWindow
	retriedEvents  rollingWindow
	abortedEvents  rollingWindow
}

func newNodeMetrics() *nodeMetrics {
	bounds := []time.Duration{100 * time.Microsecond, 500 * time.Microsecond, time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, time.Second}
	return &nodeMetrics{
		prepareLatency:              newHistogram(bounds),
		commitLatency:               newHistogram(bounds),
		endToEndTxnLatency:          newHistogram([]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}),
		fsyncLatency:                newHistogram(bounds),
		replicationRoundtripLatency: newHistogram(bounds),
		classStats:                  map[string]*txnClassStats{},
	}
}

func normalizeTxnClass(class string) string {
	class = strings.TrimSpace(class)
	if class == "" {
		return unclassifiedTxnClass
	}
	return class
}

func (m *nodeMetrics) classStatsFor(class string) *txnClassStats {
	class = normalizeTxnClass(class)
	m.classMu.Lock()
	defer m.classMu.Unlock()
	if existing, ok := m.classStats[class]; ok {
		return existing
	}
	if class != unclassifiedTxnClass && len(m.classStats) >= maxTxnClasses-1 {
		class = unclassifiedTxnClass
		if existing, ok := m.classStats[class]; ok {
			return existing
		}
	}
	created := &txnClassStats{}
	m.classStats[class] = created
	return created
}

func (m *nodeMetrics) ObserveClassRetry(class string, now time.Time) {
	stats := m.classStatsFor(class)
	stats.retried.Inc()
	stats.retriedEvents.Add(now, 1)
	m.retriedEvents.Add(now, 1)
}

func (m *nodeMetrics) ObserveClassDecision(class string, committed bool, now time.Time) {
	stats := m.classStatsFor(class)
	stats.total.Inc()
	stats.executedEvents.Add(now, 1)
	m.executedEvents.Add(now, 1)
	if committed {
		stats.committed.Inc()
	} else {
		stats.aborted.Inc()
		stats.abortedEvents.Add(now, 1)
		m.abortedEvents.Add(now, 1)
	}
}

type RateWindows struct {
	Short  float64 `json:"short"`
	Medium float64 `json:"medium"`
	Long   float64 `json:"long"`
}

func windowedRates(w *rollingWindow, now time.Time) RateWindows {
	return RateWindows{
		Short:  round2(w.Rate(rateWindowShort, now)),
		Medium: round2(w.Rate(rateWindowMedium, now)),
		Long:   round2(w.Rate(rateWindowLong, now)),
	}
}

type TxnRateGroup struct {
	ExecutedPerSec RateWindows `json:"executed_per_sec"`
	RetriedPerSec  RateWindows `json:"retried_per_sec"`
	AbortedPerSec  RateWindows `json:"aborted_per_sec"`
}

type TxnClassStatsResponse struct {
	Total     uint64       `json:"total"`
	Committed uint64       `json:"committed"`
	Aborted   uint64       `json:"aborted"`
	Retried   uint64       `json:"retried"`
	Rates     TxnRateGroup `json:"rates"`
}

type StatsResponse struct {
	NodeID uint32 `json:"node_id"`
	Role   string `json:"role"`
	Txns   struct {
		Total     uint64 `json:"total"`
		Committed uint64 `json:"committed"`
		Aborted   uint64 `json:"aborted"`
		Retried   uint64 `json:"retried"`
	} `json:"txns"`
	Rates struct {
		Txns               TxnRateGroup `json:"txns"`
		BytesWrittenPerSec RateWindows  `json:"bytes_written_per_sec"`
	} `json:"rates"`
	Redo struct {
		Entries   uint64 `json:"entries"`
		SizeBytes int64  `json:"size_bytes"`
	} `json:"redo"`
	Locks struct {
		Active int `json:"active"`
	} `json:"locks"`
	Replication struct {
		LagEntries int64 `json:"lag_entries"`
	} `json:"replication"`
	ByClass map[string]TxnClassStatsResponse `json:"by_class,omitempty"`
}

func ParseLogLevel(s string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func NewJSONLogger(level string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(logWriter{}, &slog.HandlerOptions{Level: ParseLogLevel(level)}))
}

type logWriter struct{}

func (logWriter) Write(p []byte) (int, error) { return fmt.Print(string(p)) }

func (n *Participant) roleString() string {
	switch n.mode {
	case ModeReplPrimary:
		return "primary"
	case ModeReplReplica:
		return "replica"
	default:
		return "single"
	}
}

func (n *Participant) RecordRetry() {
	n.RecordRetryForClass("")
}

func (n *Participant) RecordRetryForClass(class string) {
	n.metrics.minitransactionsRetried.Inc()
	n.metrics.ObserveClassRetry(class, n.nowFn())
}

func (n *Participant) SetLogger(logger *slog.Logger) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logger = logger
}

func (n *Participant) log(level slog.Level, msg string, args ...any) {
	if n.logger == nil {
		return
	}
	base := []any{"nodeID", n.hostId, "hostId", n.hostId, "role", n.roleString()}
	base = append(base, args...)
	n.logger.Log(context.Background(), level, msg, base...)
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func (n *Participant) StatsSnapshot() StatsResponse {
	n.mu.Lock()
	activeLocks := len(n.locks.locks)
	role := n.roleString()
	n.mu.Unlock()
	now := n.nowFn()
	var resp StatsResponse
	resp.NodeID = n.hostId
	resp.Role = role
	resp.Txns.Total = n.metrics.minitransactionsTotal.Value()
	resp.Txns.Committed = n.metrics.minitransactionsCommitted.Value()
	resp.Txns.Aborted = n.metrics.minitransactionsAborted.Value()
	resp.Txns.Retried = n.metrics.minitransactionsRetried.Value()
	resp.Rates.Txns.ExecutedPerSec = windowedRates(&n.metrics.executedEvents, now)
	resp.Rates.Txns.RetriedPerSec = windowedRates(&n.metrics.retriedEvents, now)
	resp.Rates.Txns.AbortedPerSec = windowedRates(&n.metrics.abortedEvents, now)
	resp.Rates.BytesWrittenPerSec = windowedRates(&n.metrics.bytesEvents, now)
	resp.Redo.Entries = n.metrics.redoEntriesWritten.Value()
	resp.Redo.SizeBytes = n.metrics.redoLogSizeBytes.Value()
	resp.Locks.Active = activeLocks
	resp.Replication.LagEntries = n.metrics.replicaLagEntries.Value()
	resp.ByClass = map[string]TxnClassStatsResponse{}
	n.metrics.classMu.Lock()
	for class, stats := range n.metrics.classStats {
		resp.ByClass[class] = TxnClassStatsResponse{
			Total:     stats.total.Value(),
			Committed: stats.committed.Value(),
			Aborted:   stats.aborted.Value(),
			Retried:   stats.retried.Value(),
			Rates: TxnRateGroup{
				ExecutedPerSec: windowedRates(&stats.executedEvents, now),
				RetriedPerSec:  windowedRates(&stats.retriedEvents, now),
				AbortedPerSec:  windowedRates(&stats.abortedEvents, now),
			},
		}
	}
	n.metrics.classMu.Unlock()
	return resp
}

func (n *Participant) isReadyLocked() bool {
	if n.restartRequired != nil || !n.recoveryComplete || n.backupMode {
		return false
	}
	if n.mode == ModeReplReplica && !n.replicaSynced {
		return false
	}
	return true
}

func (n *Participant) classForTIDLocked(txId sinfonia.TransactionID) string {
	if txn, ok := n.inDoubt[txId]; ok {
		return txn.class
	}
	return ""
}

func (n *Participant) QueryInDoubt(ctx context.Context, _ sinfonia.QueryInDoubtRequest) (sinfonia.QueryInDoubtResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := sinfonia.QueryInDoubtResponse{Entries: make([]sinfonia.InDoubtEntry, 0, len(n.inDoubt))}
	for txId, tx := range n.inDoubt {
		correlationID := tx.correlationID
		if correlationID == "" {
			correlationID = txId.CorrelationID()
		}
		resp.Entries = append(resp.Entries, sinfonia.InDoubtEntry{
			TID:           txId,
			PreparedAt:    tx.preparedAt.UnixNano(),
			CorrelationID: correlationID,
			Participants:  append([]uint32(nil), tx.participants...),
		})
	}
	return resp, nil
}
