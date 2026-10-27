package memnode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jbancroft/concertum2/internal/auth"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/internal/storage"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const unclassifiedTxnClass = "__unclassified__"
const maxTxnClasses = 128
const (
	rateWindowShort  = 10 * time.Second
	rateWindowMedium = time.Minute
	rateWindowLong   = 5 * time.Minute
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
	if len(r.events) > 20000 {
		r.events = append([]timedValue(nil), r.events[len(r.events)-10000:]...)
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
	authFailures              counter
	tlsHandshakeFailuresTotal counter
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

func (n *Participant) storageAccessible() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.diskImage == nil {
		return true
	}
	_, err := n.diskImage.Bytes()
	return err == nil
}

func (n *Participant) isReady() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isReadyLocked()
}

func (n *Participant) isReadyLocked() bool {
	if !n.recoveryComplete || n.backupMode {
		return false
	}
	if n.mode == ModeReplReplica && !n.replicaSynced {
		return false
	}
	return true
}

func (n *Participant) HTTPHandler(enablePrometheus bool) http.Handler {
	mux := http.NewServeMux()
	getOnly := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			h(w, r)
		}
	}
	debugWrap := func(h http.HandlerFunc) http.HandlerFunc {
		if !n.enableDebugEndpoints {
			return func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }
		}
		wrapped := h
		if n.authorizer != nil {
			wrapped = auth.RequireRole(wrapped, n.authorizer, auth.RoleAdmin)
		}
		return func(w http.ResponseWriter, r *http.Request) {
			if !n.allowRate(n.debugLimiter, "debug") {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"RESOURCE_EXHAUSTED"}`))
				return
			}
			wrapped(w, r)
		}
	}
	mux.HandleFunc("/stats", getOnly(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.StatsSnapshot())
	}))
	mux.HandleFunc("/health", getOnly(func(w http.ResponseWriter, _ *http.Request) {
		healthy := n.storageAccessible()
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": healthy, "node_id": n.hostId})
	}))
	mux.HandleFunc("/ready", getOnly(func(w http.ResponseWriter, _ *http.Request) {
		ready := n.isReady()
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": ready, "node_id": n.hostId})
	}))
	mux.HandleFunc("/debug/in_doubt", debugWrap(func(w http.ResponseWriter, _ *http.Request) {
		r, _ := n.QueryInDoubt(context.Background(), proto.QueryInDoubtRequest{})
		_ = json.NewEncoder(w).Encode(r)
	}))
	mux.HandleFunc("/debug/redo_log_head", debugWrap(func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		var entry any
		if n.redoLog != nil {
			entries := n.redoLog.Entries()
			if len(entries) > 0 {
				entry = entries[0]
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entry": entry})
	}))
	mux.HandleFunc("/debug/redo_log_inspect", debugWrap(func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		entries := []any{}
		head := any(nil)
		tail := any(nil)
		if n.redoLog != nil {
			raw := n.redoLog.Entries()
			if len(raw) > 0 {
				head = raw[0]
				tail = raw[len(raw)-1]
			}
			entries = make([]any, 0, len(raw))
			for _, e := range raw {
				decidedCommit, known := false, false
				if n.decided != nil {
					decidedCommit, known = n.decided.Get(e.TransactionId)
				}
				entries = append(entries, map[string]any{
					"tid":            e.TransactionId,
					"correlation_id": e.TransactionId.CorrelationID(),
					"participants":   e.HostIds,
					"write_count":    len(e.WriteItems),
					"known_decision": known,
					"decided_commit": decidedCommit,
				})
			}
		}
		processedPtr := 0
		if n.storageDir != "" {
			if p, err := storage.LoadProcessedPointer(n.storageDir); err == nil {
				processedPtr = p
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"head": head, "tail": tail, "entries": entries, "processed_ptr": processedPtr})
	}))
	mux.HandleFunc("/debug/txn_timeline", debugWrap(func(w http.ResponseWriter, r *http.Request) {
		txId, err := parseTIDQuery(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tid": txId, "correlation_id": txId.CorrelationID(), "events": n.TimelineForTID(txId)})
	}))
	mux.HandleFunc("/debug/forced_abort", debugWrap(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"forced_abort_current": n.metrics.forcedAbortCountCurrent.Value()})
	}))
	mux.HandleFunc("/debug/locks", debugWrap(func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"active": len(n.locks.locks), "global_write": n.locks.globalWriteLock})
	}))
	if enablePrometheus {
		mux.HandleFunc("/metrics", getOnly(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(n.prometheusMetrics()))
		}))
	}
	return mux
}

func decodeBody(r *http.Request, out any) error {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

func parseTIDQuery(r *http.Request) (proto.TransactionID, error) {
	parse := func(k string) (uint64, error) {
		v := strings.TrimSpace(r.URL.Query().Get(k))
		if v == "" {
			return 0, fmt.Errorf("missing query parameter %q", k)
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %q: %w", k, err)
		}
		return n, nil
	}
	coord, err := parse("coord")
	if err != nil {
		return proto.TransactionID{}, err
	}
	counter, err := parse("counter")
	if err != nil {
		return proto.TransactionID{}, err
	}
	epoch, err := parse("epoch")
	if err != nil {
		return proto.TransactionID{}, err
	}
	return proto.TransactionID{CoordinatorID: coord, Counter: counter, Epoch: epoch}, nil
}

func (n *Participant) prometheusMetrics() string {
	lines := []string{
		"# TYPE concertum_minitransactions_total counter",
		"concertum_minitransactions_total " + strconv.FormatUint(n.metrics.minitransactionsTotal.Value(), 10),
		"# TYPE concertum_minitransactions_committed counter",
		"concertum_minitransactions_committed " + strconv.FormatUint(n.metrics.minitransactionsCommitted.Value(), 10),
		"# TYPE concertum_minitransactions_aborted counter",
		"concertum_minitransactions_aborted " + strconv.FormatUint(n.metrics.minitransactionsAborted.Value(), 10),
		"concertum_replication_entries_sent " + strconv.FormatUint(n.metrics.replicationEntriesSent.Value(), 10),
		"concertum_replication_failures " + strconv.FormatUint(n.metrics.replicationFailures.Value(), 10),
		"concertum_log_gc_runs " + strconv.FormatUint(n.metrics.logGCRuns.Value(), 10),
		"concertum_backup_runs " + strconv.FormatUint(n.metrics.backupRuns.Value(), 10),
		"concertum_rejected_requests_total " + strconv.FormatUint(n.metrics.rejectedRequestsTotal.Value(), 10),
		"concertum_auth_failures_total " + strconv.FormatUint(n.metrics.authFailures.Value(), 10),
		"concertum_tls_handshake_failures_total " + strconv.FormatUint(n.metrics.tlsHandshakeFailuresTotal.Value(), 10),
		"concertum_rate_limited_total " + strconv.FormatUint(n.metrics.rateLimitedTotal.Value(), 10),
		"concertum_request_size_rejected_total " + strconv.FormatUint(n.metrics.requestSizeRejectedTotal.Value(), 10),
	}
	appendHist := func(name string, h *histogram) {
		cum := uint64(0)
		for i, b := range h.buckets {
			cum += h.counts[i].Load()
			lines = append(lines, fmt.Sprintf("%s_bucket{le=\"%.6f\"} %d", name, b.Seconds(), cum))
		}
		cum += h.counts[len(h.buckets)].Load()
		lines = append(lines, fmt.Sprintf("%s_bucket{le=\"+Inf\"} %d", name, cum))
		lines = append(lines, fmt.Sprintf("%s_sum %.9f", name, float64(h.sumNanos.Load())/float64(time.Second)))
		lines = append(lines, fmt.Sprintf("%s_count %d", name, h.total.Load()))
	}
	appendHist("concertum_prepare_latency_seconds", n.metrics.prepareLatency)
	appendHist("concertum_commit_latency_seconds", n.metrics.commitLatency)
	appendHist("concertum_end_to_end_txn_latency_seconds", n.metrics.endToEndTxnLatency)
	appendHist("concertum_redo_fsync_latency_seconds", n.metrics.fsyncLatency)
	appendHist("concertum_replication_roundtrip_latency_seconds", n.metrics.replicationRoundtripLatency)
	return strings.Join(lines, "\n") + "\n"
}
