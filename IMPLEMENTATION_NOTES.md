# Implementation notes

## 2026-10-27: Original gofonia vocabulary and helpers

Adapted the original transaction builder, host projections, `RangeItem`,
`Int64Set`, vote names, and range-overlap checks from gofonia. Restored names
including `MiniTransaction`, `CompareItem`, `HostId`, `Address`, `Participant`,
`inDoubtTx`, `RedoLogItem`, `DecidedTransactionLog`, `ForcedAbortLog`, and `BTree`.
The public client and SDK share the `mini_transaction` item definitions.

The working transaction, recovery, replication, and B+tree algorithms remain
derived from Concertum. The overlap scan now checks earlier long read locks
past intervening shorter readers. Existing gob registrations, protocol field
names, numeric vote values, JSON log keys, and binary page layouts are retained.
The analysis and selective reuse manifest are linked from the README.

## 2026-10-26: Coordinator test controls

Removed the coordinator's phase-two diagnostic state, pinned transaction IDs,
alternate synchronous delivery path, and replaceable clock. Delivery tests use
memory-node wrappers defined in test files, retaining the normal coordinator
workers, batching, and fallback behavior. Abrupt subprocess exit after partial
commit delivery exercises recovery from the participants' durable state.

The coordinator reads `time.Now()` when checking comparison deadlines. The
deadline predicate accepts an explicit timestamp so expiry boundaries can be
checked without changing the coordinator's clock or waiting in real time.

## 2026-10-26: Focused Go extraction

Concertum2 preserves the existing Go Sinfonia/Minuet implementation at the
revision recorded in [docs/extraction.md](docs/extraction.md). The extraction
renames the module and narrows the package set. Retained algorithms, wire
types, and on-disk formats are unchanged.

The papers were consulted for minitransaction/recovery lineage and transactional
B-tree structure. Their complete systems are broader than this sample. The
fixed page layout, whole-tree validation, single-node index placement, local
epoch convention, framed gob/TCP transport, disk format, deadline extension,
and fixed-schema SQL surface are Concertum-owned decisions inherited from the
source. [docs/design.md](docs/design.md) records their practical limits.

The loopback demo uses fresh durable state per invocation, waits for phase-two
delivery, uses disjoint memory regions for balances/index/catalog, keeps one
small SQL table, and reopens the same state to check persistence. It introduces
no new transaction or index semantics. The retained replication primitives
do not constitute a managed replicated-cluster product in this extraction.
