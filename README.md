# ConcertumDemo

A Go demo of distributed transactions, B+tree indexing, and SQL.

[Sinfonia/Minuet crash course](docs/crash-course.md) · [Design notes and limitations](docs/design.md)

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

To run a [memnode](cmd/memnode/main.go) independently:

```sh
go run ./cmd/memnode -id 1 -listen 127.0.0.1:7001
```

Add `-data-dir tmp/node-1` for persistence and `-peer ID=ADDRESS` for recovery peers.
Use a separate ID and data directory for each node. Ctrl-C stops the node.

## Minuet

Run the combined transaction, index, SQL, and durable-restart demo:

```sh
go run ./cmd/demo
```

- [B+tree](minuet-index/btree.go)
- [Combined demo](cmd/demo/main.go)

## SQL

```sh
go run ./cmd/repl

# Or save and reopen tables between sessions:
go run ./cmd/repl -data-dir tmp/repl
```

Enter `.help` for SQL examples or `.quit` to exit. Use one session per data directory.

- [Engine](minuet-sql/engine.go)
- [Catalog](minuet-sql/catalog.go)
- [Parser](minuet-sql/parser.go)
- [Planner](minuet-sql/planner.go)
- [Console](cmd/repl/main.go)

## Checks

```sh
go vet ./...
MEMNODE_BIN="$PWD/bin/memnode" go test ./...

# With the race detector:
go build -race -o bin/memnode-race ./cmd/memnode
MEMNODE_BIN="$PWD/bin/memnode-race" go test -race ./...
```

[MIT License](LICENSE)
