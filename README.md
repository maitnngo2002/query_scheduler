# query-scheduler

A distributed query scheduler for a DataFusion-based OLAP engine, written in Rust.
It splits a physical plan into fragments, dispatches them to workers, and tracks
query state. See [docs/DESIGN.md](docs/DESIGN.md) for the full design.

## Status: Phase 2b-1 (dispatch, FetchResults, end-to-end tests)

| Piece | State |
|---|---|
| gRPC API (`proto/scheduler.proto`) | Defined |
| Plan fragmenter (`crates/fragmenter`) | Prototype on a simplified plan model, with tests |
| Task expander (`crates/fragmenter/src/tasks.rs`) | Expands fragments into per-partition tasks and tracks readiness, with tests |
| Scheduler service (`crates/scheduler`) | Decodes JSON plans, expands to tasks, dispatches to workers with slot-aware assignment, tracks completion, failure, and cancel |
| Mock worker (`crates/mock-worker`) | Registers, heartbeats, fakes task execution |
| `FetchResults` and worker output service | Works end to end with mock workers (opaque bytes) |
| End-to-end tests (`crates/scheduler/tests/e2e.rs`) | In-process scheduler and test workers over real gRPC |
| DataFusion version spike (`crates/df-spike`) | Works: prints a real physical plan, round-trips it through `datafusion-proto`, and executes it |
| Plan cutter (`crates/df-adapter`) | Added: cuts a real plan into fragments and tasks (analysis only); run `cargo test -p df-adapter` |
| Shuffle nodes and real worker execution | Not yet (Phase 2b-2, slice 2b onward) |

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

`cargo test` runs unit tests in every crate plus the end-to-end tests in
`crates/scheduler/tests/e2e.rs`, which start a real scheduler and gRPC test
workers on local ports. Run just those with `cargo test -p scheduler --test e2e`.

## Run locally

In separate terminals:

```sh
# 1. scheduler (default 127.0.0.1:50051)
cargo run -p scheduler

# 2. two mock workers
WORKER_LISTEN=127.0.0.1:50061 cargo run -p mock-worker
WORKER_LISTEN=127.0.0.1:50062 cargo run -p mock-worker
```

Submit the example plan with [grpcurl](https://github.com/fullstorydev/grpcurl).
The plan is the JSON encoding of the fragmenter's plan type (an interim format
until the DataFusion adapter lands); `plan` is sent as base64 bytes:

```sh
grpcurl -plaintext -import-path proto -proto scheduler.proto \
  -d "{\"plan\": \"$(base64 < examples/join_agg.json | tr -d '\n')\"}" \
  localhost:50051 scheduler.v1.SchedulerService/SubmitQuery

grpcurl -plaintext -import-path proto -proto scheduler.proto \
  -d '{"query_id": "q-0"}' localhost:50051 scheduler.v1.SchedulerService/GetQueryStatus
```

The query should move from `QUERY_STATE_QUEUED` to `QUERY_STATE_RUNNING` to
`QUERY_STATE_SUCCEEDED`, and `GetQueryStatus` lists each task with the worker it ran on.
Once the query is `SUCCEEDED`, fetch its result stream. Mock workers return a canned
payload naming the task, so the base64 `arrowIpc` field decodes to `mock-result:q-0:4:0:0`:

```sh
grpcurl -plaintext -import-path proto -proto scheduler.proto \
  -d '{"query_id": "q-0"}' localhost:50051 scheduler.v1.SchedulerService/FetchResults
```

Scheduler settings (environment variables):

| Variable | Default | Meaning |
|---|---|---|
| `SCHEDULER_LISTEN` | `127.0.0.1:50051` | bind address |
| `SCHEDULER_SHUFFLE_PARTITIONS` | `4` | partitions per hash shuffle |
| `SCHEDULER_SCAN_SPLITS` | `1` | tasks per table scan |

## DataFusion spike

`crates/df-spike` pins DataFusion 55.1.0, writes small Parquet files, runs the
example query with 4 partitions, and prints the real physical plan, its operator
tree, and the result. It then serializes the plan with `datafusion-proto`,
decodes it, and runs the decoded plan. It is excluded from plain `cargo build`
and `cargo test` because DataFusion is a heavy build; run it explicitly:

```sh
cargo run -p df-spike
cargo test -p df-adapter
```

## Roadmap

Headline goal: near-linear scale-out speedup (see [docs/DESIGN.md](docs/DESIGN.md)).

1. **Phase 1 (done):** API, fragmenter prototype, task expansion, mock worker, skeleton service.
2. **Phase 2a/2a.1/2b-1 (done):** task dispatch with mock workers, automated end-to-end tests, worker output service and FetchResults. **Phase 2b-2:** DataFusion plan adapter and real worker execution.
3. **Phase 3:** hash shuffle between stages, dynamic task assignment, first scaling numbers.
4. **Phase 4:** full TPC-H scaling study (1, 2, 4, 8 workers), bottleneck analysis, skew handling, task retry.
5. **Phase 5:** stretch goals (pipelined stages, locality-aware placement, speculative execution).
