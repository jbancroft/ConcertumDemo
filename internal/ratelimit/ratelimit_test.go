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
