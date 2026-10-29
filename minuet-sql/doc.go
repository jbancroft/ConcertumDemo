// Package minuetsql adds a tiny SQL layer over the B+tree and transaction client.
// Tables have two unsigned columns, id and value. Supported statements are
// CREATE TABLE, INSERT, DELETE, and SELECT, with an optional ID filter for SELECT.
//
// There are no multi-statement transactions. Searches and scans use the index's
// bounded retries to load a stable tree. Each table owns a bounded region; allocation
// exhaustion returns ErrTableFull without changing stored data. See
// docs/design.md for the limits.
//
// One engine serializes writes to the same key. A duplicate insert returns
// ErrDuplicateKey, deleting a missing row returns ErrRowNotFound, and running
// out of index retries returns ErrBusy. An uncertain mutation returns
// sinfoniaclient.ErrUnknownOutcome without retrying. Tests exercise the real
// coordinator path.
package minuetsql
