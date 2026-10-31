# Sinfonia and Minuet in three minutes

Sinfonia provides atomic byte updates across servers. Minuet builds an ordered
index on that foundation.

## Sinfonia: transactions over bytes

A **memory node** is a server that owns a byte array. Addresses identify a node
and an offset. The application gives those bytes meaning: balances, tree pages,
or something else.

Sinfonia's **minitransaction** combines three operations:

- **Compare:** require bytes to match an expected value.
- **Read:** return bytes to the caller.
- **Write:** replace bytes with new values.

The caller supplies all addresses and values before execution. Reads and writes
form one atomic operation; a failed comparison aborts it without applying writes.
[Sinfonia paper, §3.3](https://read.seas.harvard.edu/~kohler/class/08w-dsi/aguilera07sinfonia.pdf#page=3).

For example, transferring 25 between balances on two nodes:

| Node | Compare balance with | Write new balance |
| --- | --- | --- |
| 1 | 100 | 75 |
| 2 | 100 | 125 |

Both changes commit together. If either balance no longer matches 100, the
transaction aborts, preventing an update based on stale data.

In this Go demo, the client library is the **coordinator**; the affected nodes
are **participants**. Execution has two phases:

1. **Prepare:** participants lock the ranges, check comparisons, collect reads,
   and, in durable mode, log the work needed for recovery before voting yes.
2. **Commit or abort:** the coordinator commits only if every participant votes
   yes. It sends the decision; participants apply committed writes and release locks.

Lock conflicts cause bounded retries. A transaction touching one node uses a
single execute-and-commit call.

After a lost reply, the client checks the original transaction's saved votes and
decisions. Nodes also finish abandoned transactions in the background: all yes
votes mean commit; a no vote means abort. Recovery never reruns the write.
If an outage outlasts the caller's deadline, the result is still **unknown**, with
a handle for checking again later. Durable nodes retain evidence across restarts.

## Minuet: an index on top

The [Minuet paper](https://www.cs.cornell.edu/~sowell/2012-VLDB-Minuet.pdf) builds
a distributed B-tree on Sinfonia, including snapshots and writable clones.

This demo implements a smaller B+tree: a sorted key/value index whose internal
pages guide searches and whose linked leaf pages support range scans. Inserts
can split pages; deletes can merge them.

An update loads pages, changes a working copy, then uses a minitransaction to
compare the original bytes and write the new ones together. This protects both
the data and the tree's structure from concurrent updates.

Each tree here occupies a bounded region on one node. It has no multiversion
snapshots or writable clones. The tiny SQL layer is an optional way to interact
with the index through the [SQL console](../README.md#sql).

## Follow the code

Build and run the [Sinfonia demo](../README.md#sinfonia),
then read its [transfer example](../demo/sinfonia-client-demo/main.go).
Then follow the [coordinator](../internal/client/coordinator.go) into participant
[prepare](../internal/memnode/prepare.go) and [commit](../internal/memnode/commit.go).
If time permits, continue to the [B+tree](../minuet-index/btree.go).
[Design notes](design.md) cover implementation details and limitations.
