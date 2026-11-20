// There are no multi-statement transactions. Searches and scans use the index's
// bounded retries to load a stable tree. Each table owns a bounded region; allocation
// exhaustion returns ErrTableFull without changing stored data. See
// docs/design.md for the limits.
//
// One engine serializes writes to the same key.
// An uncertain mutation returns sinfoniaclient.ErrUnknownOutcome without retrying.
package minuetsql
