// An uncertain update returns sinfoniaclient.ErrUnknownOutcome without retrying.
// Each tree requires an exclusive byte region; allocating beyond it returns
// ErrRegionFull before attempting a transaction. Deleted pages are not reused.
// Loading rechecks the superblock and retries concurrent changes; searches and
// scans return ErrBusy if they cannot load a stable tree within the retry budget.
//
// Minuet inspired the design, but this version has no multiversion snapshots or
// writable clones. See the README and docs/design.md for the layout and limits.
package minuetindex
