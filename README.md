# ConcertumDemo

A small Go experiment in distributed transactions, with a B+tree and a tiny SQL
layer on top. It's a code sample to read, run, and tinker with.

The idea comes from Sinfonia: describe byte ranges to **compare**, **read**, and
**write**, then commit across memory nodes. Minuet inspired the index.

## Try it

You need Go 1.22 or newer. Everything uses the standard library; there's nothing
else to install or start.

```sh
make demo
```

The demo starts two standalone memnode processes and talks to them over local TCP. It
transfers 25 between two balances, rejects an update based on a stale balance,
inserts and deletes index entries, runs a SQL query, then reopens the saved data.

```text
Two durable memnode processes listening on loopback TCP
Sinfonia: transfer committed; stale compare aborted; balances = 75, 125
Minuet: inserted 40 keys; deleted 32; ordered range = 33..40
SQL: SELECT id, value FROM scores -> [{1 100} {2 250}]
Recovery: reopened both nodes; balances, index, and SQL catalog/data verified
```

Files are left under `tmp/demo/`; use `-data-dir <parent>` to choose another spot.
The demo does an orderly restart. Tests cover crashes at specific points.
The Makefile builds `bin/memnode` before launching either demo. Each demo starts
the prebuilt executable, waits for the child nodes to finish recovery, and stops
the processes on exit. To run a demo directly, first run `make memnode`, then use
`go run ./cmd/demo` or `go run ./demo/sinfonia-client-demo` from the repository
root. Use `-memnode-bin /path/to/memnode` to select a different executable.

For a smaller example focused on the Sinfonia client API:

```sh
make sinfonia-client-demo
```

It starts two in-memory memnode processes on loopback TCP, initializes balances with direct
writes, reads them back, transfers 25 with a compare/write minitransaction, and
checks that a stale comparison aborts both writes:

```text
Two in-memory memnode processes listening on loopback TCP
Initial balances: 100, 100
After transferring 25: 75, 125
Stale comparison aborted; balances unchanged: 75, 125
```

This example keeps node data in memory. Client usage is in
[main.go](demo/sinfonia-client-demo/main.go); process
startup is shared by both demos in [democluster](internal/democluster/cluster.go).
It waits for background commit
delivery before reading results and checks transaction statuses, including an
unknown transfer outcome. It does not simulate network failures or recovery.

## Run a memnode on its own

```sh
make memnode
./bin/memnode -id 1 -listen 127.0.0.1:7001
```

You can also use `go run ./cmd/memnode` with the same flags. By default, a node
has a 2 MiB memory image and keeps data in memory. Set `-size` to change the size
in bytes, or `-data-dir` to save data across restarts. Each node needs a unique
nonzero ID and its own storage directory.

For two durable nodes, run these in separate terminals:

```sh
# Terminal 1
./bin/memnode -id 1 -listen 127.0.0.1:7001 \
  -data-dir tmp/node-1 -peer 2=127.0.0.1:7002

# Terminal 2
./bin/memnode -id 2 -listen 127.0.0.1:7002 \
  -data-dir tmp/node-2 -peer 1=127.0.0.1:7001
```

The node prints one JSON record containing its ID and RPC address when recovery
has finished and it is ready for client requests. `-listen 127.0.0.1:0` selects
a free port. Repeat `-peer ID=ADDRESS` for each other participant whose evidence
may be needed during recovery. Nodes start recovery RPC service before finishing
their own recovery, so they can restart together; recovery fails after 30 seconds
if required evidence is still unavailable. Stop a standalone node with Ctrl-C.
Shutdown cancels RPC handler contexts and closes client connections, then waits
for handlers to finish before closing storage.
Use the printed addresses in `sinfonia-client.NewClient`'s `NodeConfig` map.

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
Index and SQL mutations return `sinfoniaclient.ErrUnknownOutcome` in that case
and stop retrying. The operation may have committed; callers can recognize this
error with `errors.Is` and must not retry blindly.

## Where to read

| Start here | What to look for |
| --- | --- |
| [Demo](cmd/demo/main.go) | A complete example with checked results |
| [Sinfonia client demo](demo/sinfonia-client-demo/main.go) | Direct reads/writes, a two-node transfer, and a comparison abort |
| [Memnode command](cmd/memnode/main.go) | Standalone participant startup, recovery peers, and shutdown |
| [Sinfonia user library](sinfonia-client/client.go) | Reads, writes, transactions, and node stats from an application |
| [Transaction builder](minitxn/transaction.go) | The compare/read/write API and grouping work by host |
| [Coordinator](internal/client/coordinator.go) | Votes, retries, and sending decisions |
| [Participant](internal/memnode/participant.go) | Locks, comparisons, logging, and recovery |
| [B+tree](minuet-index/btree.go) | Pages, splits, merges, and range scans |
| [SQL engine](minuet-sql/engine.go) | A small parser and table layer over the tree |

The Sinfonia user library runs in the application process and uses the coordinator
to execute transactions across memory nodes. Index and SQL code use this library
directly.

## Keep the limits in mind

- This is experimental code for small examples. It isn't ready for production.
- Operations and HTTP endpoints have no authentication or authorization.
- A tree lives on one node, reads the whole reachable tree for updates, and never
  reuses deleted pages. Searches and scans can be inconsistent during writes.
  There are no multiversion snapshots or writable clones.
- A bad superblock currently looks like an empty tree; a later update can
  overwrite existing data.
- SQL has a few `CREATE TABLE`, `INSERT`, `SELECT`, and `DELETE` forms. No joins,
  custom schemas, or multi-statement transactions. The catalog holds six tables;
  creating another returns `ErrCatalogFull`. Table regions aren't sized for
  arbitrary growth; the demo uses one small table.
- Replication helpers are included, but automatic failover and the services to
  manage a cluster aren't. Real power-loss safety hasn't been tested here.

[Design notes](docs/design.md) go into the details and tradeoffs.

## Check it

```sh
make check       # formatting, go vet, build, and tests
make race        # tests with the Go race detector
make memnode     # build bin/memnode for standalone use
make demo
make sinfonia-client-demo
make bench       # a quick benchmark run, not a performance claim
```

The test targets build the memnode executable used by the process integration
tests; `make race` builds it with the race detector too. For direct `go test`
invocations, first build it with `make memnode` and set
`MEMNODE_BIN="$PWD/bin/memnode"` in the test environment.

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
