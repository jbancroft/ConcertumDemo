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
