// Package minuetindex implements a single-version Minuet-style B+tree over
// sinfoniaclient minitransactions. Each index resides on one memory node.
// Mutations validate and write the reachable tree; search and scan do not
// guarantee consistent snapshots alongside concurrent mutations.
//
// The fixed page format, allocator, and occupancy limits are Concertum choices.
// Research lineage, extraction provenance, and limits are documented in the
// repository README, IMPLEMENTATION_NOTES.md, and docs/design.md. This package
// does not implement the Minuet paper's multiversion snapshots or writable clones.
package minuetindex
