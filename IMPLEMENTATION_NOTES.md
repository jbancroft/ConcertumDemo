# Implementation notes

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
