.PHONY: prepare check fmt-check vet staticcheck build test race memnode demo repl sinfonia-client-demo bench

export TMPDIR := $(CURDIR)/tmp
MEMNODE_BIN := $(CURDIR)/bin/memnode
MEMNODE_RACE_BIN := $(CURDIR)/bin/memnode-race
REPL_BIN := $(CURDIR)/bin/repl
REPL_STARTUP_ATTEMPTS := 100
REPL_STARTUP_POLL_SECONDS := 0.05
STATICCHECK_VERSION := v0.8.2

prepare:
	@mkdir -p tmp

check: fmt-check vet staticcheck build test

fmt-check:
	@test -z "$$(gofmt -l cmd demo internal minitxn minuet-index minuet-sql sinfonia-client)" || \
		{ gofmt -l cmd demo internal minitxn minuet-index minuet-sql sinfonia-client; exit 1; }

vet: prepare
	go vet ./...

staticcheck: prepare
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

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
	@set -eu; \
	data_dir=$$(mktemp -d "$(TMPDIR)/repl.XXXXXX"); \
	ready="$$data_dir/ready"; \
	node_pid=; \
	trap 'if [ -n "$$node_pid" ]; then kill "$$node_pid" 2>/dev/null || :; wait "$$node_pid" 2>/dev/null || :; fi; rm -rf "$$data_dir"' EXIT; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	"$(MEMNODE_BIN)" -id 1 -listen 127.0.0.1:7001 -data-dir "$$data_dir/node" >"$$ready" & \
	node_pid=$$!; \
	attempts=0; \
	while [ ! -s "$$ready" ]; do \
		if ! kill -0 "$$node_pid" 2>/dev/null; then wait "$$node_pid"; exit 1; fi; \
		attempts=$$((attempts + 1)); \
		if [ "$$attempts" -ge "$(REPL_STARTUP_ATTEMPTS)" ]; then echo "memnode startup timed out" >&2; exit 1; fi; \
		sleep "$(REPL_STARTUP_POLL_SECONDS)"; \
	done; \
	"$(REPL_BIN)" -host 127.0.0.1 -port 7001

sinfonia-client-demo: memnode
	go run ./demo/sinfonia-client-demo -memnode-bin "$(MEMNODE_BIN)"

bench: prepare
	go test ./minuet-index -run '^$$' -bench '^BenchmarkMinuetIndex' -benchtime=1x -benchmem
