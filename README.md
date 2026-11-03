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

Requires Go 1.27+. The module selects Go 1.27.3 through automatic toolchain
switching. From the repository root, build the memnode executable used by the demos:

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
Canceling `FlushPendingCommits` stops waiting; unreported delivery errors remain
available to a later flush or `Close()`.

Transactions retry lock conflicts and stale epochs within a bounded retry budget.
A failed comparison or other non-retryable rejection aborts without retry, even
when another participant reports a lock conflict or stale epoch.
A rejection received by the coordinator remains an abort if another prepare call
fails or the caller cancels. Communication errors accompany that known outcome.
Unconfigured node IDs are rejected before contacting any participant.

Cross-node reads and comparisons retain durable votes to preserve atomicity
across node restarts. Single-node reads and comparisons do not write recovery logs.

To run a [memnode](cmd/memnode/main.go) independently:

```sh
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001 -data-dir tmp/node-1
```

Nodes keep working data in memory and persist it to the required `-data-dir`.
Give each node its own ID and directory; restart with the same directory to
recover its data. Add `-peer ID=ADDRESS` for each recovery peer. Ctrl-C stops the node.

- The memory image defaults to 2 MiB. Set `-size BYTES` when creating a node and
  use the same size on restart; existing images cannot be resized.
- Nodes are not replicated. An unavailable node can block transactions; there
  is no automatic failover or backup/restore.
- Decision updates use atomic file replacement; their I/O cost grows with
  retained transaction history.
- `ErrUnknownOutcome` means a write may have committed. Check application state
  before deciding whether to retry it.

See [storage and recovery](docs/design.md#disk-storage-and-recovery) and
[unknown outcomes](docs/design.md#unknown-outcomes) for details.

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
Within one SQL engine, inserts and deletes for the same key are serialized;
waiting for that key honors the caller's context cancellation or deadline.

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
