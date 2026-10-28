// Package minuetindex stores a small B+tree on one memory node.
// Updates compare the old page bytes and write the new ones in a minitransaction.
// Searches and scans can see inconsistent results while another caller writes.
//
// Minuet inspired the design, but this version has no multiversion snapshots or
// writable clones. See the README and docs/design.md for the layout and limits.
package minuetindex
