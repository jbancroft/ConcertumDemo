package sinfonia

import "time"

// EpochAt returns the number of UTC hours since the Unix epoch.
func EpochAt(t time.Time) uint64 {
	seconds := t.Unix()
	if seconds < 0 {
		return 0
	}
	return uint64(seconds / 3600)
}
