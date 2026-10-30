# ConcertumDemo

A small Go experiment in distributed transactions, with a B+tree and a tiny SQL
layer on top. It's a code sample to read, run, and tinker with.

The idea comes from Sinfonia: describe byte ranges to **compare**, **read**, and
**write**, then commit across memory nodes. Minuet inspired the index.

New to those names? Start with the [three-minute crash course](docs/crash-course.md)
for the core ideas, a transfer example, and a short code walkthrough.

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

It starts two in-memory memnode processes on loopback TCP, initializes balances
with transactional writes, reads them back, transfers 25 with a compare/write
minitransaction, and checks that a stale comparison aborts both writes:

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

## Try SQL interactively

```sh
make repl
```

The console starts the same two local memnode processes; SQL tables live on
node 2, matching the scripted demo. Each default session starts empty and keeps
data in memory.

```text
sql> CREATE TABLE scores;
OK
sql> INSERT INTO scores VALUES (1, 100);
OK
sql> SELECT id, value FROM scores;
id  value
1   100
(1 row)
sql> .quit
```

Enter one statement per line, with an optional trailing semicolon. `.help` shows
the supported forms. Invalid statements print an error and leave the session
open. An unknown transaction outcome is reported as potentially committed and
is never retried automatically. `.quit`, EOF, or Ctrl-C closes the session and
stops its child processes. Input lines must be shorter than 64 KiB; multiline
statements, history, and completion aren't implemented.

`make repl` also builds `bin/repl`. To save tables and reopen them in later
sessions, run this command from the repository root:

```sh
./bin/repl -data-dir tmp/repl
```

Use one session per data directory. The directory contains both nodes' saved
files. Omitting `-data-dir` starts a fresh in-memory session. Use
`-memnode-bin /path/to/memnode` to choose another prebuilt node executable.
You can also pass the saved `tmp/demo/run-...` directory printed by `make demo`
to query its `scores` table.
The six-table catalog and fixed 64 KiB table regions apply in the console too.

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

Each participant acquires its transaction's complete set of range locks together.
Reads and comparisons protect their full ranges, including bytes outside any
overlapping writes in the same transaction. A conflict with another transaction
rejects the attempt; overlapping operations within one transaction do not
conflict with each other.

All application reads and writes use this transaction API. `Client.Read` and
`Client.Write` submit a single read or write item as a minitransaction, with the
same locking rules on RAM and durable nodes. There are no raw data-access RPCs.
Separate reads are separate snapshots; the B+tree still validates the pages it
loads. SQL uses the tree for two columns: `id` and `value`.

The convenience methods return `sinfoniaclient.ErrTransactionAborted` for a
definite rejection. Read-only calls currently save transaction decisions and
replay responses too, so reads on durable nodes incur disk writes.

Commit messages normally run in the background; `FlushPendingCommits` waits for
queued delivery. After an ambiguous RPC error, the client automatically checks
the original transaction's votes and decisions. Each memnode also checks old
prepared transactions and finishes them even if the coordinator disappeared.
A failed decision-log write retains pending writes and locks for recovery; the
node requires successful restart recovery before serving ordinary requests again.
Recovery also saves original read results still held by a pending transaction
before clearing it, preserving them for later resolution and durable restarts.

An outage or deadline can still leave an unknown outcome. Both transaction APIs,
including the builder, return an `ExecResult` with a `Recovery` handle. Save the
handle and call `Client.Resolve(ctx, handle)` later; it checks the same attempt
without rerunning the write. The handle can be serialized as JSON and used with
a new client configured for the same nodes.

Read/write, index, and SQL errors preserve that handle in
`*sinfoniaclient.UnknownOutcomeError` (`errors.As`); `errors.Is(err,
sinfoniaclient.ErrUnknownOutcome)` still identifies uncertainty. Resolve before
retrying a mutation. See [recovery details and a short example](docs/design.md#resolving-an-unknown-outcome)
for deadlines, evidence retention, and missing read results.

## Where to read

| Start here | What to look for |
| --- | --- |
| [Demo](cmd/demo/main.go) | A complete example with checked results |
| [Sinfonia client demo](demo/sinfonia-client-demo/main.go) | Transactional reads/writes, a two-node transfer, and a comparison abort |
| [SQL REPL](cmd/repl/main.go) | Interactive queries, readable results, and local node cleanup |
| [Memnode command](cmd/memnode/main.go) | Standalone participant startup, recovery peers, and shutdown |
| [Sinfonia user library](sinfonia-client/client.go) | Reads, writes, transactions, and node stats from an application |
| [Transaction builder](minitxn/transaction.go) | The compare/read/write API and grouping work by host |
| [Coordinator](internal/client/coordinator.go) | Votes, retries, and sending decisions |
| [Participant](internal/memnode/participant.go) | Shared state, construction, and private byte-access helpers |
| [Prepare](internal/memnode/prepare.go) | Range locks, comparisons, reads, and durable yes votes |
| [Commit](internal/memnode/commit.go) | Commit/abort decisions and the single-participant path |
| [Recovery coordinator](internal/recovery/resolve.go) | Collecting votes and delivering recovery decisions |
| [Participant recovery](internal/memnode/recovery.go) | Saved decisions, epoch floors, and peer evidence |
| [B+tree](minuet-index/btree.go) | Pages, splits, merges, and range scans |
| [SQL engine](minuet-sql/engine.go) | A small parser and table layer over the tree |

The Sinfonia user library runs in the application process and uses the coordinator
to execute transactions across memory nodes. Index and SQL code use this library
directly.

## Keep the limits in mind

- This is experimental code for small examples. It isn't ready for production.
- Operations and HTTP endpoints have no authentication or authorization.
- A tree lives on one node, reads the whole reachable tree for updates, and never
  reuses deleted pages. Reads retry if a writer changes the tree during loading
  and return `ErrBusy` if the retry budget runs out.
  There are no multiversion snapshots or writable clones.
- Trees require an explicit byte region. Nonzero invalid metadata and changed
  region bounds are rejected; all-zero metadata still means unused storage.
- SQL has a few `CREATE TABLE`, `INSERT`, `SELECT`, and `DELETE` forms. No joins,
  custom schemas, or multi-statement transactions. The catalog holds six tables;
  creating another returns `ErrCatalogFull`. Each table has a fixed region
  (64 KiB by default). A split that cannot fit returns `ErrTableFull` without
  changing storage. `CREATE TABLE` returns `ErrStorageFull` if the configured
  node memory cannot fit another region. Regions do not grow or reclaim pages.
- Replication helpers are included, but automatic failover and the services to
  manage a cluster aren't. Real power-loss safety hasn't been tested here.

When constructing a SQL engine, set `MemorySize` to the node's usable memory
limit and optionally set `TableRegionSize` for new tables. SQL owns table storage
from byte 1024 up to that limit. Saved tables retain their original bounds when
reopened, even if the default region size changes. Direct index users supply
`BaseAddress` and `RegionSize` to `NewBTree`, which now returns a tree and an error.

The bounded tree and catalog formats require fresh data; earlier saved images
are rejected. `make demo` creates a fresh directory on each run.

[Design notes](docs/design.md) go into the details and tradeoffs.

## Check it

```sh
make check       # formatting, go vet, build, and tests
make race        # tests with the Go race detector
make memnode     # build bin/memnode for standalone use
make demo
make repl        # interactive SQL console
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
