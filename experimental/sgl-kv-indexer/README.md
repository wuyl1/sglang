# SGL KV Indexer

`sgl-kv-indexer` is an experimental shared metadata index for SGLang KV-cache
blocks. It records which worker and storage tier currently holds each
content-addressed block, allowing a router to query likely cache hits without
moving KV data itself.

## Architecture

```text
SGLang worker ── ZMQ PUB + replay ROUTER ──> bridge ── gRPC ──> indexer ──> Redis
```

- SGLang publishes `BlockStored`, `BlockRemoved`, and `AllBlocksCleared` events.
- One `kv-indexer-bridge` follows each independent worker/rank event stream.
- `kv-indexer-server` applies ordered event batches and serves match queries.
- Redis, Dragonfly, or Redis Cluster stores durable placement metadata.

The bridge detects sequence gaps and requests missing batches from SGLang's
replay endpoint. The indexer uses per-worker sequence gating and incarnation
fencing so duplicate or delayed batches cannot overwrite newer state.

## Current status and limitations

This crate is experimental.

- Multiple indexer servers may share one Redis namespace when worker ownership
  is statically partitioned between them. All events for one worker must go to
  exactly one indexer server at a time because per-worker apply serialization is
  process-local.
- Deploy one bridge per independent SGLang KV-event stream (for example, per DP
  rank), each with a unique `KV_INDEXER_WORKER_ID`.
- Configure the replay endpoint in production, and size SGLang's `buffer_steps`
  to cover the longest expected bridge outage. Recovery is best effort: a gap
  the buffer can no longer cover retires the worker incarnation, so the indexer
  drops that worker's placements and the bridge resyncs from the live stream.
- Redis Cluster is supported, but an apply batch spans multiple hash slots and
  is not globally atomic. Per-worker sequence and generation fencing preserve
  replay convergence.

## Horizontal scaling

Indexer servers share no persistent local state, so they can scale horizontally
by statically assigning each worker's bridge to one server while all servers use
the same Redis namespace:

```text
bridge(worker-0) ──> indexer-0 ─┐
bridge(worker-1) ──> indexer-1 ─┼─> shared Redis namespace
bridge(worker-2) ──> indexer-0 ─┤
bridge(worker-3) ──> indexer-1 ─┘
```

Set each bridge's existing `KV_INDEXER_ENDPOINT` to its assigned server. Match
queries can be sent to any indexer server because every server reads the shared
Redis state.

The deployment must guarantee that two indexer servers do not concurrently
process events for the same `KV_INDEXER_WORKER_ID`. Automatic endpoint
selection, rebalancing, and failover are intentionally out of scope; reassign
and restart the affected bridge when changing ownership.

## Build

```bash
cd experimental/sgl-kv-indexer
cargo build --release --features redis-backend
```

This produces:

- `target/release/kv-indexer-server`
- `target/release/kv-indexer-bridge`

The `redis-backend` feature is required to run the Redis-backed server.

## Run locally

Start Redis:

```bash
redis-server --port 6379
```

Start the indexer:

```bash
KV_INDEXER_BACKEND=redis \
KV_INDEXER_REDIS_URL=redis://127.0.0.1:6379 \
KV_INDEXER_LISTEN_ADDR=127.0.0.1:50051 \
RUST_LOG=info \
  cargo run --release --features redis-backend --bin kv-indexer-server
```

Start one bridge for a SGLang worker:

```bash
KV_INDEXER_WORKER_ID=worker-0 \
KV_INDEXER_WORKER_ADDRESS=http://127.0.0.1:30000 \
KV_INDEXER_ENDPOINT=http://127.0.0.1:50051 \
SGLANG_KV_EVENT_ENDPOINT=tcp://127.0.0.1:5567 \
SGLANG_KV_EVENT_REPLAY_ENDPOINT=tcp://127.0.0.1:5590 \
RUST_LOG=info \
  cargo run --release --bin kv-indexer-bridge
```

The corresponding SGLang server must publish KV events at those endpoints, for
example with a `kv-events-config` containing a ZMQ publisher endpoint and replay
endpoint.

## Configuration

### Indexer server

| Variable | Default | Description |
| --- | --- | --- |
| `KV_INDEXER_LISTEN_ADDR` | `[::1]:50051` | gRPC listen address |
| `KV_INDEXER_BACKEND` | required | `redis` for the production backend |
| `KV_INDEXER_REDIS_URL` | none | Single Redis/Dragonfly URL |
| `KV_INDEXER_REDIS_CLUSTER_NODES` | none | Comma-separated Redis Cluster seed URLs; takes precedence over the single URL |
| `KV_INDEXER_REDIS_NAMESPACE` | `kvidx` | Redis key prefix |
| `KV_INDEXER_REDIS_REQUIRED` | `1` | Fail startup if Redis is unavailable; `0` enables degraded lazy connection |
| `KV_INDEXER_WORKER_TTL_SECS` | `120` | Worker liveness TTL; `0` disables expiry |

### Bridge

| Variable | Default | Description |
| --- | --- | --- |
| `KV_INDEXER_WORKER_ID` | required | Unique ID for this worker event stream |
| `KV_INDEXER_WORKER_ADDRESS` | empty | Address returned to match callers |
| `KV_INDEXER_ENDPOINT` | `http://[::1]:50051` | Indexer gRPC endpoint |
| `SGLANG_KV_EVENT_ENDPOINT` | `tcp://127.0.0.1:5557` | SGLang event PUB endpoint |
| `SGLANG_KV_EVENT_REPLAY_ENDPOINT` | none | SGLang replay ROUTER endpoint |
| `SGLANG_KV_EVENT_TOPIC` | empty | ZMQ subscription topic |
| `KV_INDEXER_CLEAR_TIERS` | `HBM,DRAM,SSD` | Tiers affected by `AllBlocksCleared` |
| `KV_INDEXER_HEARTBEAT_SECS` | `30` | Worker heartbeat interval; `0` disables it |
| `KV_INDEXER_WORKER_INCARNATION` | generated | Optional observable prefix for generated incarnation tokens |
| `KV_INDEXER_WORKER_INCARNATION_FILE` | `/tmp/sgl-kv-indexer-<worker-id-hex>.incarnation` | Local checkpoint that preserves the publisher incarnation across bridge-only restarts; place it on a sidecar-persistent volume when Bridge and SGLang have different container lifecycles. Best effort: an unwritable location only costs a full resync on restart, it never blocks startup |

## API

The protobuf service in `proto/kv_indexer.proto` provides:

- `ApplyExternalKvBatch`: ordered placement reports, revocations, and clears.
- `MatchExternalKv`: workers and tiers holding requested block hashes.
- `GetExternalKvHitCounts`: per-block hit counters.

The server also exposes the standard gRPC health service. Redis readiness is
checked periodically and reflected as `SERVING` or `NOT_SERVING`.

## Tests

Single Redis:

```bash
KV_INDEXER_REDIS_URL=redis://127.0.0.1:6379 \
  cargo test --features redis-backend -- --test-threads=1
```

Static checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features redis-backend -- -D warnings
```

The repository CI additionally starts a six-node Redis Cluster and exercises
normal Cluster routing, ASK redirects, and master failover.
