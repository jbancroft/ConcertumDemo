package memnode

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/jbancroft/concertum2/internal/auth"
	"github.com/jbancroft/concertum2/internal/ratelimit"
)

var (
	ErrResourceExhausted = errors.New("RESOURCE_EXHAUSTED")
	ErrUnauthorized      = errors.New("UNAUTHORIZED")
	ErrForbidden         = errors.New("FORBIDDEN")
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
	Authorizer   *auth.Authorizer
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
		cfg.Burst = 100
	}
	n.prepareLimiter = ratelimit.NewKeyedLimiter(cfg.PrepareRate, cfg.Burst)
	n.commitLimiter = ratelimit.NewKeyedLimiter(cfg.CommitRate, cfg.Burst)
	n.recoveryLimiter = ratelimit.NewKeyedLimiter(cfg.RecoveryRate, cfg.Burst)
	n.debugLimiter = ratelimit.NewKeyedLimiter(cfg.DebugRate, cfg.Burst)
	n.authorizer = cfg.Authorizer
	n.enableDebugEndpoints = cfg.EnableDebug
}

func (n *Participant) authorize(ctx context.Context, allowed ...auth.Role) error {
	if n.authorizer == nil {
		return nil
	}
	id, ok := auth.FromContext(ctx)
	if !ok {
		n.metrics.authFailures.Inc()
		return ErrUnauthorized
	}
	if n.authorizer.Policy != nil {
		action := callerAction()
		resource := fmt.Sprintf("memnode/%d", n.hostId)
		if err := n.authorizer.Authorize(ctx, action, resource, allowed...); err != nil {
			n.metrics.authFailures.Inc()
			return ErrForbidden
		}
		return nil
	}
	for _, r := range allowed {
		if id.Role == r {
			return nil
		}
	}
	n.metrics.authFailures.Inc()
	return ErrForbidden
}

func callerAction() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return "memnode.unknown"
	}
	name := runtime.FuncForPC(pc).Name()
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimPrefix(name, "(*Node).")
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return "memnode." + strings.ToLower(b.String())
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
