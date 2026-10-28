.PHONY: prepare check fmt-check vet build test race demo bench

export TMPDIR := $(CURDIR)/tmp

prepare:
	@mkdir -p tmp

check: fmt-check vet build test

fmt-check:
	@test -z "$$(gofmt -l cmd internal minitxn minuet-index minuet-sql pkg sinfonia-client)" || \
		{ gofmt -l cmd internal minitxn minuet-index minuet-sql pkg sinfonia-client; exit 1; }

vet: prepare
	go vet ./...

build: prepare
	go build ./...

test: prepare
	go test ./...

race: prepare
	go test -race ./...

demo: prepare
	go run ./cmd/demo

bench: prepare
	go test ./minuet-index -run '^$$' -bench '^BenchmarkMinuetIndex' -benchtime=1x -benchmem
