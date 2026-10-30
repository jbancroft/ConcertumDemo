.PHONY: prepare check fmt-check vet build test race memnode demo repl sinfonia-client-demo bench

export TMPDIR := $(CURDIR)/tmp
MEMNODE_BIN := $(CURDIR)/bin/memnode
MEMNODE_RACE_BIN := $(CURDIR)/bin/memnode-race
REPL_BIN := $(CURDIR)/bin/repl

prepare:
	@mkdir -p tmp

check: fmt-check vet build test

fmt-check:
	@test -z "$$(gofmt -l cmd demo internal minitxn minuet-index minuet-sql sinfonia-client)" || \
		{ gofmt -l cmd demo internal minitxn minuet-index minuet-sql sinfonia-client; exit 1; }

vet: prepare
	go vet ./...

build: memnode
	go build ./...

test: memnode
	MEMNODE_BIN="$(MEMNODE_BIN)" go test ./...

race: prepare
	go build -race -o "$(MEMNODE_RACE_BIN)" ./cmd/memnode
	MEMNODE_BIN="$(MEMNODE_RACE_BIN)" go test -race ./...

memnode: prepare
	go build -o "$(MEMNODE_BIN)" ./cmd/memnode

demo: memnode
	go run ./cmd/demo -memnode-bin "$(MEMNODE_BIN)"

repl: memnode
	go build -o "$(REPL_BIN)" ./cmd/repl
	"$(REPL_BIN)" -memnode-bin "$(MEMNODE_BIN)"

sinfonia-client-demo: memnode
	go run ./demo/sinfonia-client-demo -memnode-bin "$(MEMNODE_BIN)"

bench: prepare
	go test ./minuet-index -run '^$$' -bench '^BenchmarkMinuetIndex' -benchtime=1x -benchmem
