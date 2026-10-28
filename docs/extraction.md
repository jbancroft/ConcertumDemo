# Extraction provenance

- Source: `github.com/jbancroft/concertum`
- Source branch: `main`
- Source revision: `06e300eff8a8e4c3ce947fbbe4c6c5e25405ba42`
- Extraction date: 2026-10-26
- Latest Go change in that revision's history: `f82fb5e361aeabbc793677d8fd36a56b4e7b1c31`,
  2026-08-28, `test: cover mixed topology planning`.

The source checkout was clean and its HEAD matched GitHub's `main` when the
extraction was made. This is a curated copy with fresh repository history;
the initial commit date records packaging of the sample, not when all the
underlying code was originally written.

## Retained source

All top-level Go files in these package directories were copied, except eight
test files that import excluded infrastructure:

```text
internal/auth          internal/client       internal/memnode
internal/proto         internal/ratelimit    internal/rpcwire
internal/storage      pkg/concertum         sinfonia-client
minuet-index           minuet-sql
```

At extraction, the 83 retained source files were unchanged except for replacing
the import prefix
`github.com/jbancroft/concertum/` with `github.com/jbancroft/concertum2/` and
normalizing constant alignment with `gofmt` in `minuet-index/index_benchmark_test.go`.
The package comment in `minuet-index/doc.go` is rewritten to describe this
sample's actual scope and point to its local documentation.
[source-manifest.json](source-manifest.json) records both SHA-256 hashes for
every retained file and the exact omitted test paths/dependencies. This
includes existing node replication, fencing, backup, and directory-client
code where it is interwoven with the core; it does not include their manager
or daemon deployment surfaces.

The helper in `internal/memnode/rpc_test_helpers_test.go` is adapted from the
excluded `replication_control_test.go` so the retained direct-write RPC test
remains runnable. `cmd/demo/`, the Makefile, `.gitignore`, and this focused
documentation were added during extraction. `go.mod` keeps the source's Go
1.22 minimum and uses the new module path. The extracted packages use only
the Go standard library, so there is no `go.sum`.

The extraction changes packaging and examples rather than transaction,
recovery, wire, disk, or index algorithms. The new documentation deliberately
limits compatibility claims to what the retained implementation demonstrates.
No original Git history, issue database, local configuration, credentials,
personal contact details, paper PDFs, release artifacts, or GitHub Actions
workflows were copied. The source snapshot had no license file, and the initial
extraction added no licensing grant.

## Changes after extraction

On 2026-10-26, an [MIT License](../LICENSE) was added for this repository.

On 2026-10-26, the coordinator's phase-two diagnostic configuration and injected
clock were removed. Phase-two tests now wrap the existing memory-node interface
to control delivery through the normal asynchronous workers, including batched
and fallback commits. A subprocess exits without cleanup after one participant
commits, then the test reopens both durable nodes and verifies recovery.
Deadline validation uses the real clock and a pure function tested with fixed
timestamps. The former diagnostic test file was replaced by
`internal/client/phase_two_test.go`; deadline cases are in
`internal/client/deadline_test.go`.

The source manifest records the original extraction snapshot and hashes, before
these changes; it is not a hash inventory of the current working tree.

On 2026-10-27, transaction helpers and naming were adapted from the separate
private gofonia repository, without changing that repository. The current
participant and B+tree implementations are in `internal/memnode/participant.go`
and `minuet-index/btree.go`. The original extraction manifest retains their old
filenames as historical evidence. See [gofonia-lineage.md](gofonia-lineage.md)
and [gofonia-source-manifest.json](gofonia-source-manifest.json) for the exact
source revision, reused code, renames, and compatibility decisions.

On 2026-10-28, the README, design notes, and code comments were rewritten in
plainer language. The README also describes the AI assistance behind much of the
newer code. No executable Go code changed. Both source manifests still describe
their recorded snapshots, so current comments can differ from their file hashes.

On 2026-10-28, one merge commit's author details were changed to the GitHub alias
and no-reply address. That changed its ID and the IDs of later commits, while
preserving their file contents. Local snapshot references were updated to the
corrected history; the source repositories and recorded file hashes are unchanged.

On 2026-10-28, the project was renamed from Concertum2 to ConcertumDemo. The Go
module and imports now use `github.com/jbancroft/ConcertumDemo`. Earlier names in
these notes and the manifests describe the historical snapshots.
Gob message registrations keep their original package prefix for compatibility;
the saved prepare-frame fixture and outgoing message-name check cover that case.

## Interview use

The demo and reading guide make this a smaller entry point into existing work.
Pair it with a description of the components you worked on and the design
tradeoffs you can discuss. The source dates above distinguish earlier work
from the later packaging of this sample.
