package storage

import "github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"

// TransactionEpochWindow admits unfamiliar IDs from the current and previous UTC hour.
const TransactionEpochWindow uint64 = 2

// TransactionRetention decides which transaction outcomes are too old to keep.
// An unpinned outcome can be dropped when Epoch + RetryHorizon <= CurrentEpoch,
// matching the node's rule for rejecting old retries.
// A zero RetryHorizon keeps everything. The caller supplies an epoch that only
// moves forward; the store doesn't guess it from the clock.
type TransactionRetention struct {
	// CurrentEpoch is the caller's view of the current retry epoch.
	CurrentEpoch uint64
	// RetryHorizon is the retry window in epochs. Zero keeps all outcomes.
	RetryHorizon uint64
	// Keep pins records still needed by in-flight transactions or recovery.
	Keep map[sinfonia.TransactionID]struct{}
}

// Expired says whether a transaction is outside the retry window and not pinned.
// A zero RetryHorizon disables expiration.
func (r TransactionRetention) Expired(txId sinfonia.TransactionID) bool {
	if _, keep := r.Keep[txId]; keep {
		return false
	}
	if r.RetryHorizon == 0 {
		return false
	}
	// Subtract instead of adding to Epoch so uint64 can't overflow.
	// If the current epoch hasn't reached the retry window, keep everything.
	if r.CurrentEpoch < r.RetryHorizon {
		return false
	}
	return txId.Epoch <= r.CurrentEpoch-r.RetryHorizon
}
