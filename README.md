# ConcertumDemo

A Go demo of distributed transactions, B+tree indexing, and SQL, based on
[Sinfonia](https://read.seas.harvard.edu/~kohler/class/08w-dsi/aguilera07sinfonia.pdf)
and [Minuet](https://www.cs.cornell.edu/~sowell/2012-VLDB-Minuet.pdf).

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

To run a [memnode](cmd/memnode/main.go) independently:

```sh
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001
```

Add `-data-dir tmp/node-1` for persistence and `-peer ID=ADDRESS` for recovery peers.
Use a separate ID and data directory for each node. Ctrl-C stops the node.
The image size defaults to 2 MiB; use `-size BYTES` to choose another size when
creating a node. Reopen durable nodes with the same size: a mismatch fails startup
without resizing the image. Resizing existing images is not supported.

Malformed decision-log records block startup with an error identifying the file
and line. The log is preserved for manual repair; automatic repair is not implemented.
Embedded participants must stop serving requests and join their recovery workers
before calling `Participant.Close()`, which drains and stops the redo appender.

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
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001

# Or save and reopen tables between memnode restarts:
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001 -data-dir tmp/repl
```

Then connect from another terminal:

```sh
go run ./cmd/repl

# Optional host and port (these are the defaults):
go run ./cmd/repl -host 127.0.0.1 -port 7001
```

Enter `.help` for SQL examples or `.quit` to exit. Exiting the REPL leaves the
memnode running; data lasts until that node stops unless it uses `-data-dir`.
Use one memnode per data directory and one SQL session per node.

For a temporary local session, `make repl` builds both executables, starts one
in-memory memnode on `127.0.0.1:7001`, and connects the console. It stops that node
and discards its data when the console exits. The port must be free.

- [Engine](minuet-sql/engine.go)
- [Catalog](minuet-sql/catalog.go)
- [Parser](minuet-sql/parser.go)
- [Planner](minuet-sql/planner.go)
- [Console](cmd/repl/main.go)

## Checks

```sh
make check       # Formatting, go vet, Staticcheck, build, and tests
make staticcheck # Run only Staticcheck

# With the race detector:
make race
```

[MIT License](LICENSE)
