package memnode

import (
	"errors"

	"github.com/jbancroft/ConcertumDemo/internal/ratelimit"
)

var ErrResourceExhausted = errors.New("RESOURCE_EXHAUSTED")

const (
	defaultMaxInFlightPrepare = 256
	defaultRateLimitBurst     = 100
)

type SafetyConfig struct {
	MaxInFlightPrepare  int
	MaxInDoubtSoft      int
	MaxInDoubtHard      int
	MaxRequestBytes     int
	MaxItemsPerTxn      int
	MaxReadBytesPerTxn  int64
	MaxWriteBytesPerTxn int64

	PrepareRate  float64
	CommitRate   float64
	RecoveryRate float64
	DebugRate    float64
	Burst        int
	EnableDebug  bool
}

func (n *Participant) ConfigureSafety(cfg SafetyConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if cfg.MaxInFlightPrepare > 0 {
		n.maxInFlightPrepare = cfg.MaxInFlightPrepare
		n.prepareSlots = make(chan struct{}, cfg.MaxInFlightPrepare)
	}
	if cfg.MaxInDoubtSoft > 0 {
		n.maxInDoubtSoft = cfg.MaxInDoubtSoft
	}
	if cfg.MaxInDoubtHard > 0 {
		n.maxInDoubtHard = cfg.MaxInDoubtHard
	}
	if cfg.MaxRequestBytes > 0 {
		n.maxRequestBytes = cfg.MaxRequestBytes
	}
	if cfg.MaxItemsPerTxn > 0 {
		n.maxItemsPerTxn = cfg.MaxItemsPerTxn
	}
	if cfg.MaxReadBytesPerTxn > 0 {
		n.maxReadBytesPerTxn = cfg.MaxReadBytesPerTxn
	}
	if cfg.MaxWriteBytesPerTxn > 0 {
		n.maxWriteBytesPerTxn = cfg.MaxWriteBytesPerTxn
	}
	if cfg.Burst <= 0 {
		cfg.Burst = defaultRateLimitBurst
	}
	n.prepareLimiter = ratelimit.NewKeyedLimiter(cfg.PrepareRate, cfg.Burst)
	n.commitLimiter = ratelimit.NewKeyedLimiter(cfg.CommitRate, cfg.Burst)
	n.recoveryLimiter = ratelimit.NewKeyedLimiter(cfg.RecoveryRate, cfg.Burst)
	n.debugLimiter = ratelimit.NewKeyedLimiter(cfg.DebugRate, cfg.Burst)
	n.enableDebugEndpoints = cfg.EnableDebug
}

func (n *Participant) allowRate(l *ratelimit.KeyedLimiter, key string) bool {
	if l == nil {
		return true
	}
	ok := l.AllowN(key, 1)
	if !ok {
		n.metrics.rateLimitedTotal.Inc()
		n.metrics.rejectedRequestsTotal.Inc()
	}
	return ok
}
