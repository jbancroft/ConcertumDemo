# ConcertumDemo

A Go demo of distributed transactions, B+tree indexing, and SQL, based on [Sinfonia](https://read.seas.harvard.edu/~kohler/class/08w-dsi/aguilera07sinfonia.pdf) and [Minuet](https://www.cs.cornell.edu/~sowell/2012-VLDB-Minuet.pdf).

ConcertumDemo is an experimental Go project exploring how a small distributed transaction primitive can support higher-level data structures.

I’ve long been interested in Sinfonia’s approach: providing atomic operations over bytes distributed across memory nodes, letting applications build structures and services on that foundation. The original research demonstrated applications including a cluster filesystem. I wanted to explore those ideas using Go. I chose Go because its static typing, efficient memory usage, and concurrency model seemed like good fits for the needs of this project. For encoding/decoding, gob appealed to me as a straightforward way to transfer messages between Go processes. Familiarity influenced that choice too—I encountered it when first learning Go, and it became a natural default.

This demo connects cross-node transactions and durable recovery to a B+tree and a small SQL layer. Each tree currently lives on one node. Distributed trees and a filesystem for sharing agent context across machines are possible future directions; their scalability and usefulness remain open questions here.

[Sinfonia/Minuet crash course](docs/crash-course.md) · [Design notes and limitations](docs/design.md)

## Run with Docker

From the repository root:

```sh
docker build -t concertum-demo .
docker run --rm --network none concertum-demo
```

Runs the combined transaction, index, SQL, and durable-restart demo as a non-root
user. Both nodes communicate over loopback inside the container. No host Go
installation, mounted directories, or published ports are needed. Demo data is
removed with the container when it exits.

## Run locally

Requires Go 1.22+. From the repository root, build the memnode executable used by the demos:

```sh
go build -o bin/memnode ./cmd/memnode
```

## Sinfonia

```sh
go run ./demo/sinfonia-client-demo
```

The transfer demo uses durable nodes in a temporary directory and removes that
directory after stopping the nodes.

- [Client API](sinfonia-client/client.go)
- [Minitransaction builder](minitxn/transaction.go)
- [Transaction coordinator](internal/client/coordinator.go)
- [Participant](internal/memnode/participant.go): [prepare](internal/memnode/prepare.go), [commit](internal/memnode/commit.go)
- [Recovery coordinator](internal/recovery/resolve.go)
- [Participant recovery](internal/memnode/recovery.go)
- [Transfer demo](demo/sinfonia-client-demo/main.go)

After all client calls have returned, call `Client.Close()` to drain queued
decisions and release RPC workers and connections. Check its error for failed
delivery; closing does not undo a known commit. Only repeated `Close` calls are
allowed afterward. The demos and REPL close their clients automatically.

Transactions retry lock conflicts and stale epochs within a bounded retry budget.
A failed comparison or other non-retryable rejection aborts without retry, even
when another participant reports a lock conflict or stale epoch.
Unconfigured node IDs are rejected before contacting any participant.

To run a [memnode](cmd/memnode/main.go) independently:

```sh
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001 -data-dir tmp/node-1
```

Every node is durable and requires `-data-dir`. It keeps its working data in
memory and persists its image and recovery records to that directory. Add
`-peer ID=ADDRESS` for recovery peers. Use a separate ID and data directory for
each node; reopen the same directory to recover its data. Ctrl-C stops the node.
Nodes are not replicated and there is no automatic failover; an unavailable node
can block transactions until it returns. Backup and restore are not implemented.
Nodes accept at most 256 concurrent transaction-execution RPCs. Commit and
recovery requests are exempt from this limit.
The image size defaults to 2 MiB; use `-size BYTES` to choose another size when
creating a node. Reopen nodes with the same size: a mismatch fails startup
without resizing the image. Resizing existing images is not supported.

`redo.log` uses framed gob records to store transaction writes for recovery.
Existing JSON redo logs are unsupported; use a fresh data directory when upgrading
from that format. Original replies are cached only in memory and are lost on
restart. No replay log is created or read; an old `replay.log` is unused and can be
deleted. An ambiguous request returns `ErrUnknownOutcome` without retrying the
operation. Nodes still recover unfinished transactions, but there is no public
transaction-history query or `Resolve` API. Callers must not retry uncertain
writes blindly.
Read-only and compare-only minitransactions append neither redo nor decision
records. Their original outcomes and read results are not recoverable after a
participant restart; issue a fresh read-only transaction if needed.

Malformed or unterminated decision and forced-abort records block startup with an
error identifying the file and line. The log is preserved for manual repair;
automatic repair is not implemented.
An uncertain redo or forced-abort append faults the node until storage is reopened
and recovery succeeds; retrying recovery on the same participant cannot clear it.
Embedded participants use `RunRecovery` for background recovery and reply expiration.
Stop serving requests and join that worker before calling `Participant.Close()`.
Recovery treats expired transactions without retained evidence as unknown.
Redo cleanup removes each decision after durably removing its redo entry.
Committed entries remain until all participants have applied their writes.
Startup finishes decision cleanup if a crash interrupts those steps. Collected
transactions have no retained history or duplicate-execution guarantee.
Applied acknowledgments are currently kept only in memory. If one participant
collects a transaction and another restarts before collecting it, redo cleanup on
the restarted node can stall indefinitely, retaining subsequent entries too.

## Minuet

Run the combined transaction, index, SQL, and durable-restart demo:

```sh
go run ./cmd/demo
```

- [B+tree](minuet-index/btree.go)
- [Combined demo](cmd/demo/main.go)

## SQL

Make sure a memnode with ID 1 and the default 2 MiB size is running:

```sh
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001 -data-dir tmp/repl
```

Then connect from another terminal:

```sh
go run ./cmd/repl

# Optional host and port (these are the defaults):
go run ./cmd/repl -host 127.0.0.1 -port 7001
```

Enter `.help` for SQL examples or `.quit` to exit. Exiting the REPL leaves the
memnode running; restart that node with the same data directory to reopen tables.
Use one memnode per data directory and one SQL session per node.

For a temporary local session, `make repl` builds both executables, starts one
durable memnode on `127.0.0.1:7001` in a temporary directory, and connects the
console. It stops that node and removes its data directory when the console
exits. The port must be free.

- [Engine](minuet-sql/engine.go)
- [Catalog](minuet-sql/catalog.go)
- [Parser](minuet-sql/parser.go)
- [Console](cmd/repl/main.go)

## Checks

```sh
make check       # Formatting, go vet, Staticcheck, build, and tests
make staticcheck # Run only Staticcheck

# With the race detector:
make race
```

[MIT License](LICENSE)
