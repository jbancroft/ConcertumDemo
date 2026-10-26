package ratelimit

import (
	"math"
	"sync"
	"time"
)

type TokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func New(rate float64, burst int) *TokenBucket {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst <= 0 {
		return nil
	}
	now := time.Now()
	return &TokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: now}
}

func (b *TokenBucket) AllowN(n int) bool {
	if b == nil {
		return true
	}
	need := float64(n)
	if need <= 0 {
		need = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens < need {
		return false
	}
	b.tokens -= need
	return true
}

type KeyedLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   int
	buckets map[string]*TokenBucket
}

func NewKeyedLimiter(rate float64, burst int) *KeyedLimiter {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst <= 0 {
		return nil
	}
	return &KeyedLimiter{rate: rate, burst: burst, buckets: map[string]*TokenBucket{}}
}

func (l *KeyedLimiter) AllowN(key string, n int) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	b := l.buckets[key]
	if b == nil {
		b = New(l.rate, l.burst)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	return b.AllowN(n)
}
