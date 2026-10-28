# ConcertumDemo

A small Go experiment in distributed transactions, with a B+tree and a tiny SQL
layer on top. It's a code sample to read, run, and tinker with.

The idea comes from Sinfonia: describe byte ranges to **compare**, **read**, and
**write**, then commit across memory nodes. Minuet inspired the index.

## Try it

You need Go 1.22 or newer. Everything uses the standard library; there's nothing
else to install or start.

```sh
go run ./cmd/demo
```

The demo starts two nodes in one process and talks to them over local TCP. It
transfers 25 between two balances, rejects an update based on a stale balance,
inserts and deletes index entries, runs a SQL query, then reopens the saved data.

```text
Two durable participants listening on loopback TCP
Sinfonia: transfer committed; stale compare aborted; balances = 75, 125
Minuet: inserted 40 keys; deleted 32; ordered range = 33..40
SQL: SELECT id, value FROM scores -> [{1 100} {2 250}]
Recovery: reopened both nodes; balances, index, and SQL catalog/data verified
```

Files are left under `tmp/demo/`; use `-data-dir <parent>` to choose another spot.
The demo does an orderly restart. Tests cover crashes at specific points.

## How it fits together

1. A `MiniTransaction` collects operations at known addresses. A comparison says,
   for example, "only make these changes if the balance is still 100."
2. The coordinator asks each participant to prepare. Participants lock ranges,
   check comparisons, and log prepared writes when using disk storage.
3. If everyone votes yes, the coordinator sends commit messages. Participants
   apply writes and release locks. Saved evidence lets recovery finish after a crash.

The B+tree stores pages through this transaction API. SQL uses the tree for two
columns: `id` and `value`.

Commit messages normally run in the background; `FlushPendingCommits` waits for
delivery. A network failure can leave the caller unsure whether a transaction
committed, so the API also has an explicit unknown result.

## Where to read

| Start here | What to look for |
| --- | --- |
| [Demo](cmd/demo/main.go) | A complete example with checked results |
| [Transaction builder](minitxn/transaction.go) | The compare/read/write API and grouping work by host |
| [Coordinator](internal/client/coordinator.go) | Votes, retries, and sending decisions |
| [Participant](internal/memnode/participant.go) | Locks, comparisons, logging, and recovery |
| [B+tree](minuet-index/btree.go) | Pages, splits, merges, and range scans |
| [SQL engine](minuet-sql/engine.go) | A small parser and table layer over the tree |

The [client](pkg/concertum/client.go) and [SDK](sinfonia-client/client.go) connect
these layers. Index and SQL code use the SDK without importing node internals.

## Keep the limits in mind

- This is experimental code for small examples. It isn't ready for production.
- Operations and HTTP endpoints have no authentication or authorization.
- A tree lives on one node, reads the whole reachable tree for updates, and never
  reuses deleted pages. Searches and scans can be inconsistent during writes.
  There are no multiversion snapshots or writable clones.
- A bad superblock currently looks like an empty tree; a later update can
  overwrite existing data.
- SQL has a few `CREATE TABLE`, `INSERT`, `SELECT`, and `DELETE` forms. No joins,
  custom schemas, or multi-statement transactions. Table regions aren't sized
  for arbitrary growth; the demo uses one small table.
- Replication helpers are included, but automatic failover and the services to
  manage a cluster aren't. Real power-loss safety hasn't been tested here.

[Design notes](docs/design.md) go into the details and tradeoffs.

## Check it

```sh
make check       # formatting, go vet, build, and tests
make race        # tests with the Go race detector
make demo
make bench       # a quick benchmark run, not a performance claim
```

## Where it came from

This grew out of my earlier gofonia experiments. Much of the newer code was built
with AI assistance, with me directing many design choices. The working engine
came from Concertum; helpers and names also came from gofonia.

The papers behind the ideas are
[Sinfonia](https://read.seas.harvard.edu/~kohler/class/08w-dsi/aguilera07sinfonia.pdf)
and [Minuet](https://www.cs.cornell.edu/~sowell/2012-VLDB-Minuet.pdf).
This project implements a small subset of those ideas, with its own storage
formats and wire protocol.

Licensed under the [MIT License](LICENSE).
