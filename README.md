# raft-kv

A fault-tolerant, replicated key-value store built on a from-scratch implementation
of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go.

No consensus library. Leader election, log replication, persistence, and snapshotting
are all implemented directly against the paper.

[![CI](https://github.com/shlok1806/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/shlok1806/raft-kv/actions/workflows/ci.yml)

---

## Why

Using etcd or Consul teaches you their API. Implementing Raft teaches you why the
algorithm is shaped the way it is - why terms are monotonic, why a leader may not
commit an entry from a previous term on replica count alone, why election timeouts
must be randomized. The hard parts of this repo are the parts that only show up
under partition and crash, which is why the test suite spends most of its time
there rather than on the happy path.

## What works

| Capability | Status | Where |
|---|---|---|
| Leader election, randomized timeouts | ✅ | `raft/raft.go` - `electionLoop`, `startElection` |
| Log replication with consistency check | ✅ | `raft/raft.go` - `AppendEntries`, `sendAppendEntries` |
| Commit advance via majority match index | ✅ | `raft/raft.go` - `maybeAdvanceCommit` |
| Crash recovery from persisted state | ✅ | `raft/persist.go` - `FilePersister` |
| Log compaction / snapshots | ✅ implemented, ⚠️ off in the server binary | `raft/snapshot.go`, `kvserver.snapshot` |
| Linearizable reads | ✅ reads go through the log, not the local map | `kvserver/server.go` - `Get` |
| Exactly-once client semantics | ✅ dedup table keyed by `ClientId → RequestId` | `kvserver/server.go` - `apply` |
| Cluster membership changes | ❌ not implemented - membership is static | - |

## Architecture

```
cmd/server ──┐
             ├── transport/  net/rpc over TCP; one server hosts both services
cmd/client ──┘
                    │
                    ▼
              kvserver/     state machine: map[string]string + dedup table
                    │       submits ops, waits for them to be applied
                    ▼
                raft/       consensus: election, replication, persistence,
                            snapshotting. Knows nothing about keys or values.
```

The layering is the point: `raft/` has no idea it is replicating a key-value store.
It replicates opaque commands and reports them on an apply channel. `kvserver/`
interprets them.

| Package | Responsibility |
|---|---|
| `raft/` | Consensus core. Election, replication, persistence, snapshots. |
| `kvserver/` | Replicated state machine and the client-facing API. |
| `transport/` | `net/rpc` dispatch between nodes. |
| `config/` | Cluster config and timeout defaults. |
| `cmd/server`, `cmd/client` | Binaries. |

## Running a 5-node cluster

Start five nodes, each with the full peer list and its own index:

```bash
go build -o bin/server ./cmd/server
for i in 0 1 2 3 4; do
  ./bin/server -id $i \
    -peers localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004 \
    -data data &
done
```

Then use the client. It retries against each server until it finds the leader, so
any address in the list works:

```bash
go build -o bin/client ./cmd/client
PEERS=localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004

./bin/client -peers $PEERS put city chicago
./bin/client -peers $PEERS get city        # -> chicago
./bin/client -peers $PEERS delete city
./bin/client -peers $PEERS get city        # -> (not found)
```

To watch a failover, kill whichever node logs `became leader`, then read again -
the client re-routes to the new leader and the value survives.

Each node persists to `data/node<id>/`. Delete that directory to reset a node.

## Configuration

Defaults live in `config/DefaultConfig()`:

| Setting | Default | Notes |
|---|---|---|
| `ElectionTimeoutMin` / `Max` | 150ms / 300ms | Randomized per node to avoid split votes |
| `HeartbeatInterval` | 50ms | Must stay well below the election minimum |
| `SnapshotThreshold` | 100 entries | See the limitation below |
| `DataDir` | `data` | Per-node subdirectory |

## Testing

```bash
go test ./... -race          # full suite, with the race detector
go test ./tests/ -run TestChaos -v
```

The suite runs in-process: RPCs are dispatched as direct method calls through a
test harness that can partition and heal individual nodes, so failures are
deterministic to set up and fast to run.

| Test | What it exercises |
|---|---|
| `TestInitialElection` | Exactly one leader emerges from a cold start |
| `TestReelection` | A new leader is elected after the current one is isolated |
| `TestTermMonotonicity` | Terms never decrease across three successive leader failures |
| `TestBasicAgreement` | An entry replicates to all peers |
| `TestNetworkPartition` | A minority partition cannot commit; the majority can; one leader after heal |
| `TestBasicKV` | Get / Put / Delete round trip |
| `TestDuplicateRequests` | A retried request is applied exactly once |
| `TestConcurrentClients` | Concurrent writers do not corrupt the state machine |
| `TestCrashRecovery` | State survives a node restart from persisted data |
| `TestLeaderCrashMidWrite` | A leader crash mid-write does not lose an acknowledged entry |
| `TestChaos` | Randomized partition / crash / heal cycles |

Run with `-race`. For a consensus implementation the race detector is not
optional - most of the interesting bugs are concurrency bugs.

## Known limitations

These are real, and listed deliberately rather than hidden.

1. **Snapshotting is disabled in the server binary.** `cmd/server/main.go` constructs
   the KV server with `maxRaftState = -1`, which means "never snapshot". Compaction
   is implemented and covered by tests, but a long-running node's log grows without
   bound. Note also that `maxRaftState` is measured in *bytes* while
   `config.SnapshotThreshold` is measured in *entries*, so the two are not
   interchangeable - wiring them together needs a deliberate choice of unit.
2. **Membership is static.** The peer list is fixed at startup. There is no
   add/remove-server support, so the cluster cannot be resized without a full restart.
3. **No TLS or authentication.** `transport/` speaks plain `net/rpc` over TCP.
   Intended for a trusted network or local experimentation.
4. **Reads go through the log.** This buys linearizability at the cost of latency.
   There is no lease-based or read-index fast path.
5. **In-memory state machine.** The KV map is rebuilt from the log and snapshots on
   restart; it is not backed by an on-disk store.

## Reference

- [In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout)](https://raft.github.io/raft.pdf)
- The extended paper's Figure 2 is the specification this implementation follows.

## License

MIT - see [LICENSE](LICENSE).
