// Package minuetsql contains an MVP SQL layer over Minuet indexes and Sinfonia-backed minitransactions.
//
// Current MVP scope:
//   - Catalog/table/index bootstrap for SQL tables.
//   - Minimal parser/planner/executor for CREATE TABLE, INSERT, DELETE, and SELECT.
//   - End-to-end tests that exercise behavior through real Sinfonia coordinator paths.
//
// MVP isolation contract (repository extension semantics):
//   - Per-statement atomicity only: each SQL statement maps to one bounded Sinfonia/minuet-index mutation/read flow.
//   - No multi-statement transactions yet; there is no BEGIN/COMMIT/ROLLBACK surface in this package.
//   - Same-engine, same-primary-key write conflicts are serialized; concurrent INSERT on one key yields one success and one ErrDuplicateKey.
//   - DELETE on a missing row is deterministic (ErrRowNotFound), including when another concurrent statement has already removed the row.
//   - Retry exhaustion from lower minuet-index commit conflicts is surfaced as ErrBusy.
package minuetsql
