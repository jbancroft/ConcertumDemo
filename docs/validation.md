# Validation

Validated on 2026-10-26 with Go 1.26.6 on macOS/arm64.

| Check | Result |
| --- | --- |
| `make check` (formatting, vet, build, all package tests) | Passed |
| `make race` (all package tests with race detection) | Passed |
| `make demo` | Passed; TCP transfer/abort, index, SQL, durable reopen |
| `make bench` | Passed; one iteration per deterministic index benchmark |
| Retained-source SHA-256 comparison | Passed; import rename, one formatting normalization, package-comment update recorded |
| Local documentation links | Passed |
| Publication file scan | No personal contact details, local home paths, credential patterns, workflows, or tracker data found |

The new demo test exercises real coordinator-to-participant TCP requests and
reopens the same durable files. Existing tests cover additional recovery and
replication primitives. This validation does not cover a multi-host deployment,
automatic failover, power-loss testing, or Minuet's full multiversion system.
The one-iteration benchmarks are a smoke check, not performance evidence.

After the coordinator test controls were removed on 2026-10-26, `make check`
and `make race` passed again. Replacement tests exercise ordinary asynchronous
delivery, generated transaction IDs, abort delivery, and abrupt subprocess exit
after partial commit delivery through both batching and individual fallback.
Both durable participants are reopened to verify recovery completes the writes.
Comparison deadlines are checked at fixed times before, at, and after expiry.
The source-hash comparison above describes the initial extraction snapshot;
subsequent changes are recorded in [extraction.md](extraction.md).

## 2026-10-27: Gofonia integration

Validated with Go 1.26.6 on macOS/arm64 after adopting the original gofonia
transaction vocabulary and helpers.

| Check | Result |
| --- | --- |
| `make check` | Passed; formatting, vet, build, and all package tests |
| `make race` | Passed; all package tests with race detection |
| `make demo` | Passed; TCP transfer/abort, B+tree, SQL, and durable reopen |
| `make bench` | Passed; one iteration per deterministic index benchmark |
| Compatibility fixtures | Passed; pre-integration gob prepare frame and JSON redo record |
| Adapted-code regression tests | Passed; builder ownership/projections, ID sets, vote values, range overlap, nested-reader locking |
| Gofonia provenance | Selected source and destination hashes verified; original source revision unchanged |
| Documentation links and targeted privacy scan | Passed; no contact addresses, home paths, private keys, or known credential-token patterns found |

The source-level interface names changed. RPC method strings and registrations,
stored record keys, numeric votes, and binary page layouts remain compatible.
The benchmarks remain smoke checks rather than performance claims. The original
gofonia server was not run or modified; its comparison was based on the source
snapshot described in [gofonia-lineage.md](gofonia-lineage.md).

The separate prepared-state naming follow-up passed `make check` and `make race`
on 2026-10-27. The updated destination hashes and original source hashes were
verified again; the original gofonia checkout remained unchanged.
