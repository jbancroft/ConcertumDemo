# Concertum2

A focused Go code sample extracted from Concertum: Sinfonia-style distributed
minitransactions, durable memory nodes, a Minuet-style B+tree, and a small SQL
layer. The engine code and retained tests come from the existing Go project;
the extraction adds a runnable demo and review documentation.

Go 1.22 or newer. No third-party dependencies or external services are required.

## Run it

```sh
go run ./cmd/demo
```

The demo starts two durable memory nodes and sends coordinator requests over
loopback TCP. It commits a conditional transfer across both participants,
rejects a stale comparison, grows and shrinks an index, executes a SQL query,
then closes and reopens both nodes to verify persisted state.

```text
Two durable participants listening on loopback TCP
Sinfonia: transfer committed; stale compare aborted; balances = 75, 125
Minuet: inserted 40 keys; deleted 32; ordered range = 33..40
SQL: SELECT id, value FROM scores -> [{1 100} {2 250}]
Recovery: reopened both nodes; balances, index, and SQL catalog/data verified
```

Each invocation creates a fresh subdirectory under `tmp/demo/` and retains it
for inspection. Use `-data-dir <parent>` to choose another location. Both nodes
run in the demo process; recovery-peer calls are local, and client data traffic
uses the retained framed gob/TCP implementation. Reopening is an orderly
restart demonstration; participant crash/recovery cases live in the tests.

## Where to start reading

| File | What it demonstrates |
| --- | --- |
| [Demo](cmd/demo/main.go) | How the layers fit together, with checked outcomes |
| [Coordinator](internal/client/coordinator.go) | Parallel prepare, votes, retries, one-participant fast path, asynchronous decision delivery |
| [Memory node](internal/memnode/node.go) | Range locking, conditional execution, durable decisions, idempotency, recovery |
| [Storage](internal/storage/) | Disk image, redo log, replay records, durable metadata |
| [Public API](pkg/concertum/client.go) | Transaction builder and explicit unknown outcomes |
| [Index](minuet-index/index.go) | B+tree layout, search, splits, borrowing/merging, ordered scans |
| [SQL engine](minuet-sql/engine.go) | Catalog-backed tables over the index |

The index and SQL layers depend on `sinfonia-client`, which adapts the public
transaction API. They do not import memory-node internals.

## Verify

```sh
make check       # formatting, vet, build, and tests
make race        # all tests with the Go race detector
make demo
make bench       # one iteration of each deterministic index benchmark
```

You can also use `go test ./...`, `go test -race ./...`, `go vet ./...`, and
`go build ./...` directly. The Make targets keep temporary test files under
the ignored repository-local `tmp/` directory. Tests include compare failure,
lock contention, replay/idempotency, ambiguous outcomes, durable recovery,
replication primitives, malformed input, index rebalance invariants, SQL
conformance, and the complete loopback demo.

## Scope and limits

This is an implementation sample with deliberately bounded scope. It retains
the existing Go algorithms, including their tradeoffs:

- Minitransactions operate on known byte ranges. Ordinary distributed commits
  return once votes establish the decision; call `FlushPendingCommits` when
  immediate phase-two delivery matters. Transport ambiguity can produce an
  unknown outcome.
- An index uses one memory node, fixed 512-byte pages, `uint64` keys and values,
  and eight keys per node. Mutations read the reachable tree and validate/write
  those pages in a minitransaction. The page allocator grows monotonically.
  This favors inspectability over scale.
- Search and scan use ordinary page reads without a final validation pass.
  They do not provide a guaranteed consistent snapshot during concurrent
  mutations. There is no MVCC, copy-on-write snapshot, writable clone, proxy
  cache, or sharding of one tree across nodes.
- SQL supports `CREATE TABLE name`, `INSERT INTO name VALUES (id, value)`,
  `SELECT id, value FROM name` with an optional `WHERE id = n`, and
  `DELETE FROM name WHERE id = n`. It has no joins, arbitrary schemas, or
  multi-statement transactions. The fixed catalog holds six tables; reserved
  index regions are small and are not an allocator for independently growing
  tables. The demo deliberately uses one small SQL table.
- Node replication/fencing and directory-client primitives remain because
  they are part of the retained core. The manager, directory daemon, automatic
  failover, and deployment lifecycle are outside this sample.

See [design and tradeoffs](docs/design.md) and
[extraction provenance](docs/extraction.md) for the exact boundary.

## Research lineage

[Sinfonia: a new paradigm for building scalable distributed systems](https://read.seas.harvard.edu/~kohler/class/08w-dsi/aguilera07sinfonia.pdf)
(SOSP 2007) motivates the compare/read/write primitive and participant recovery.
[Minuet: A Scalable Distributed Multiversion B-Tree](https://www.cs.cornell.edu/~sowell/2012-VLDB-Minuet.pdf)
(VLDB 2012), especially its background on transactional B-trees in Section 2,
provides the index lineage. This sample implements a smaller single-version
index and does not claim the complete Minuet system or its published performance.
The page format, RPC encoding, storage layout, deadline extension, and SQL
surface are Concertum implementation choices.
