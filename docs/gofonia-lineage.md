# Original gofonia analysis and integration

## Source and scope

Source: `github.com/jbancroft/gofonia`, branch `master`, revision
`8e9284b78fa5e1a7032f09eafbd072d0b596bc29` (2025-01-25, `Implement minuet`).
Integration date: 2026-10-27.

The source was inspected in a temporary checkout. No files, commits, branches,
settings, or other state in the original repository were changed. The current
ConcertumDemo code was the destination for all adaptations. The source manifest
records selected source-file hashes and how each file informed this integration.
Its target hashes describe `target_revision`, including the naming follow-up
below. Later edits can change file hashes. `integration_revision`
identifies the first integration commit before that follow-up.

Gofonia contains the beginnings of the same architecture: a minitransaction
builder partitions compares, reads, and writes by host; a coordinator collects
prepare votes; participants retain in-doubt state and redo/decision/forced-abort
logs; recovery contacts the transaction's other hosts. It also contains range
locking, transport helpers, and an incomplete Minuet experiment.

The runnable transaction/storage/index engine in this repository remains
derived from Concertum. Gofonia contributes adapted helpers and terminology.
The source revision and original dates are preserved; renaming does not change
the chronology of the original work.

## Reused code

| Original gofonia code | Current use | Adaptations |
| --- | --- | --- |
| `mini_transaction/mini_transaction.go` | `mini_transaction/mini_transaction.go` | Builder and per-host projections use pointer receivers, copied byte slices, context-aware execution, and an explicit executor instead of global hosts. |
| `compare_item.go`, `read_item.go`, `write_item.go` | `mini_transaction/items.go` | Retains `CompareItem`, `ReadItem`, `WriteItem`, `HostId`, `Address`, `Length`, and `Offset`/`Size`. Host IDs and addresses retain unsigned widths; compare/write lengths come from `Data`. |
| `mini_transaction/int64_set.go` | Removed after the initial integration | Host discovery now uses local `map[uint32]struct{}` sets. `UniqueHostIds` returns sorted `[]uint32` IDs without converting them to `int64`. |
| `mini_transaction/vote.go` | `mini_transaction/vote.go`, re-exported internally by `proto` | Retains `VoteOk` and printable names. Existing numeric wire values are preserved; `VoteUnknown` uses `-1` rather than displacing the existing success value. |
| `mutex/range_lock.go` | `RangeLock.OverlapsWith` in the participant | Adapts the overlap predicate to existing endpoint-based locks and handles empty ranges explicitly. The existing participant mutex protects lock state. |
| Range-lock tests | Corresponding participant tests | Retains useful boundary cases and adds the nested-reader regression. |

`Compares`, `Reads`, `Writes`, `CompareItemsForHost`, `ReadItemsForHost`,
`WriteItemsForHost`, and `UniqueHostIds` return detached snapshots. Changing a
returned collection cannot change the transaction. Execution also receives its
own operation bytes. Gofonia's original builder returned slices into its state.

The `Int64Set` wrapper and its tests were
removed; transaction tests cover unique host IDs, the unsigned ID range, and
returned-slice ownership. The source manifest still records the earlier snapshot,
including its original paths and set implementation.

The range-lock scan previously stopped when it encountered any earlier lock
whose end preceded the requested write. A still-earlier reader could extend
past that shorter reader and overlap the write. The scan now continues checking
those earlier locks, with a regression test for nested compatible readers.

## Naming map

| Previous Concertum2 name | Current name or shape |
| --- | --- |
| `MTx`, `NewMTx` | `MiniTransaction`, `NewMiniTransaction` |
| `CmpItem`, `Cmp`, `CmpValidUntil` in the public client and SDK | `CompareItem`, `Compare`, `CompareValidUntil` |
| `ExecAndCommit`, `ExecAndCommitDetailed` | `ExecuteAndCommit`, `ExecuteAndCommitDetailed` |
| Public item fields `MemID`, `Addr`, `Len` | `HostId`, `Address`, `Length` |
| `memnode.Node`, `NewNode`, `NewDurableNode` | `memnode.Participant`, `NewParticipant`, `NewDurableParticipant` |
| `MemNode`, `RPCMemNode` client handles | `Participant`, `RPCParticipant` |
| `ExecPrepare` Go methods | `ExecAndPrepare` |
| `preparedTxn`, `preparedAt`, its `participants` | Retained as `preparedTxn`, `preparedAt`, `participants` |
| `redoLog`, `forcedAbort`, `allLogTIDs` participant fields | `redoLog` retained; `forcedAborts`, `allLogTxIds` |
| `RedoEntry`, `DecidedStore`, `ForcedAbortStore` | `RedoLogItem`, `DecidedTransactionLog`, `ForcedAbortLog` |
| Log insertion and forced-abort lookup | `AddItem`, `IsForcedAbort` |
| `Index`, `New`, internal `node` in `minuet-index` | `BTree`, `NewBTree`, `BTreeNode` |
| B+tree `Leaf`, `Keys`, `Children` | `isLeaf`, `keys`, `children` |

The first integration commit used gofonia's `inDoubtTx`, `createdAt`, `hostIds`,
and `redo` for participant state. A separate naming follow-up restored
`preparedTxn`, `preparedAt`, `participants`, and `redoLog`, along with
`logParticipants` for the logged transaction's participant list. The transaction
builder, public item vocabulary, participant methods, and B+tree names remain
as adapted from gofonia.

These are source-level interface changes. Repository callers, the demo, and
tests use the current names. There is no layer of deprecated aliases for the
former method names. The client and SDK expose aliases to the one canonical
set of public transaction item definitions.

## Working behavior retained

- The coordinator creates fresh structured IDs for retry attempts. The original
  builder's random string IDs and retry scheduling are not imported.
- Context cancellation, bounded backoff, explicit unknown outcomes, one-participant
  execution, asynchronous decision delivery, replay, and durable recovery remain.
- Gofonia's `ReadIfCompareFails` flag was not implemented by its participant and
  is not added as a nonfunctional option. Its unused version-table and transaction
  interfaces are not imported.
- RPC method strings, registered gob request/response names, wire item fields
  (`MemID`, `Addr`, `Len`), and structured ID fields retain their existing layout.
  Internal wire records deliberately remain separate from public item types.
- `RedoLogItem.TransactionId` and `HostIds` retain JSON keys `tid` and
  `participants`. Redo writes retain the internal wire item representation.
- Image filenames, decision/abort logs, replay formats, numeric vote values,
  and B+tree/catalog page formats are preserved. Fixed fixtures captured from
  Concertum2 `8611b32` guard the old prepare-frame and redo-record layouts.
- The B+tree remains a single-node, single-version, unsigned integer key/value
  index. Gofonia's byte-slice key skeleton is not a compatible replacement.

## Problems found in the original snapshot

These findings come from source inspection. The original server was not started
or tested in place: its package initializer creates a data file and allocates a
large buffer. Running it would be inappropriate for a read-only comparison.

- The builder's mutating methods use value receivers. Slice append assignments
  update a copy, so operations can disappear even as the shared host set changes.
- The retry timer calls `ExecuteAndCommit` on the same transaction ID, while the
  original call returns an abort without waiting for the retry's result.
- Recovery enqueues votes only for successful RPCs but waits for the full host
  count, so failed calls can leave it waiting indefinitely.
- The decided-transaction writer emits gob values directly while its reader
  expects a binary length prefix. The formats do not agree.
- The data manager uses `os.Create` during construction, which truncates an
  existing image, and its asynchronous writes do not provide the current durable
  decision/apply guarantees.
- The custom `TryLock` can wait inside `RWMutex.Lock` after acquiring its channel
  token, rather than providing the current non-waiting range acquisition.
- Minuet declares `Key` as `[]byte` but compares keys using `<`, `>`, and `==`;
  those expressions are not valid Go slice comparisons. It also has unused
  imports and lacks the current persistent B+tree's delete, scan, and rebalance
  implementation.
- The root contains both `main` and `gofonia` package files. Several transport
  setters also use value receivers, losing connection or identity updates.

The global host registry, data-file initializer, custom mutex, log I/O, transport
implementation, CLI binary, private configuration, and unfinished modules were
therefore not copied. The current standard-library transport and durable engine
remain responsible for those behaviors.

The generic disk-queue interfaces and file-writer experiment also have no active
consumer in this engine. They were left out instead of adding unused queue or
file-lifecycle code alongside the existing durable redo/replay implementation.
