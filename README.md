# query-scheduler

A distributed query scheduler for a DataFusion-based OLAP engine, written in Rust.
It splits a physical plan into fragments, dispatches them to workers, and tracks
query state. See [docs/DESIGN.md](docs/DESIGN.md) for the full design.

## Status: Phase 1 (skeleton)

| Piece | State |
|---|---|
| gRPC API (`proto/scheduler.proto`) | Defined |
| Plan fragmenter (`crates/fragmenter`) | Prototype on a simplified plan model, with tests |
| Scheduler service (`crates/scheduler`) | Runs; worker registry, heartbeats, eviction, query state machine |
| Mock worker (`crates/mock-worker`) | Registers, heartbeats, fakes fragment execution |
| Plan decoding, dispatch, results | Not yet (Phase 2) |

## Layout

```
proto/                    API definitions
crates/scheduler-proto    generated tonic/prost code
crates/fragmenter         plan -> fragment DAG
crates/scheduler          scheduler service
crates/mock-worker        fake worker for development
docs/DESIGN.md            design document
```

## Prerequisites

* Rust (stable) via [rustup](https://rustup.rs)
* `protoc` (Protocol Buffers compiler)
  * macOS: `brew install protobuf`
  * Ubuntu/Debian: `sudo apt-get install protobuf-compiler`

## Build and test

```sh
cargo build
cargo test
```

## Run locally

In separate terminals:

```sh
# 1. scheduler (default 127.0.0.1:50051)
cargo run -p scheduler

# 2. two mock workers
WORKER_LISTEN=127.0.0.1:50061 cargo run -p mock-worker
WORKER_LISTEN=127.0.0.1:50062 cargo run -p mock-worker
```

Submit a query with [grpcurl](https://github.com/fullstorydev/grpcurl). `plan` is
base64 bytes; the skeleton only checks that it is non-empty:

```sh
grpcurl -plaintext -import-path proto -proto scheduler.proto \
  -d '{"plan": "AAEC"}' localhost:50051 scheduler.v1.SchedulerService/SubmitQuery

grpcurl -plaintext -import-path proto -proto scheduler.proto \
  -d '{"query_id": "q-0"}' localhost:50051 scheduler.v1.SchedulerService/GetQueryStatus
```

## Roadmap

1. **Phase 1 (this):** API, fragmenter prototype, mock worker, skeleton service.
2. **Phase 2:** DataFusion plan adapter, dispatch fragments, feed task status into query state, FetchResults.
3. **Phase 3:** concurrency, retries, cancellation propagation, priority policy, TPC-H benchmarks.
4. **Phase 4:** stretch goals (locality-aware placement, speculative execution).
