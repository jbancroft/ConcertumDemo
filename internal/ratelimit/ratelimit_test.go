package ratelimit

import (
	"math"
	"testing"
)

func TestConstructorsRejectNonFiniteRates(t *testing.T) {
	for _, rate := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if New(rate, 1) != nil {
			t.Errorf("New(%v) returned a limiter", rate)
		}
		if NewKeyedLimiter(rate, 1) != nil {
			t.Errorf("NewKeyedLimiter(%v) returned a limiter", rate)
		}
	}
}

func TestKeyedLimiterTracksKeysIndependently(t *testing.T) {
	limiter := NewKeyedLimiter(1, 1)
	if !limiter.AllowN("a", 1) || !limiter.AllowN("b", 1) {
		t.Fatal("each key should receive its own initial burst")
	}
	if limiter.AllowN("a", 1) {
		t.Fatal("key a burst should be exhausted")
	}
}

func TestNilKeyedLimiterAllows(t *testing.T) {
	var limiter *KeyedLimiter
	if !limiter.AllowN("any", 100) {
		t.Fatal("nil limiter should allow")
	}
}

func FuzzLimiterNumericInputsNeverPanic(f *testing.F) {
	f.Add(1.0, 1, 1, "key")
	f.Add(math.NaN(), 1, 1, "")
	f.Add(math.Inf(1), 1, -1, "key")
	f.Add(0.0, 0, 0, "key")
	f.Fuzz(func(t *testing.T, rate float64, burst, request int, key string) {
		bucket := New(rate, burst)
		keyed := NewKeyedLimiter(rate, burst)
		invalid := rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst <= 0
		if invalid {
			if bucket != nil || keyed != nil {
				t.Fatal("invalid limiter configuration was accepted")
			}
			return
		}
		_ = bucket.AllowN(request)
		_ = keyed.AllowN(key, request)
	})
}
