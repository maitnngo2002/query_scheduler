# Distributed Query Scheduler

**Author:** _Your Name_
**Stack:** Rust, Apache DataFusion, tonic (gRPC), Arrow Flight, tokio

A personal project to build a standalone query scheduler for a distributed, cloud-native OLAP engine built on Apache DataFusion.

## Overview

The scheduler is a standalone service that turns a physical query plan into a distributed execution across many worker nodes. It receives an Apache DataFusion physical plan, splits it into fragments, decides where and when each fragment runs, tracks progress and failures, and streams the final result set back to the client.

**What it does**

1. Breaks a DataFusion physical plan into fragments (stages) connected by data exchanges.
2. Dispatches fragments to worker nodes and coordinates data movement between them.
3. Handles concurrent queries, worker membership, failures, and cancellation.
4. Returns results to the client.

**Non-goals:** query optimization, writing a new execution engine (workers wrap DataFusion), transactions, security and authentication.

**Why this project:** distributed scheduling is a compact way to practice fragmenting plans, concurrency, failure handling, and service API design, with results that are easy to benchmark.

### Milestones

* **Milestone 1 (core):** Fragment plans at exchange boundaries. Dispatch to a static list of workers round-robin, one query at a time. Match single-node DataFusion results on a subset of TPC-H.
* **Milestone 2 (robust):** Concurrent queries, dynamic worker registration with heartbeats, fragment retry on failure, cancellation, and a priority-based scheduling policy. Full TPC-H (SF 1) correctness and a scaling benchmark.
* **Stretch goals:** Locality-aware placement using a local file cache, cost-based priorities, speculative re-execution of stragglers, and a push vs. pull scheduling comparison.

## Architectural Design

**Input:** a serialized DataFusion physical plan (via `datafusion-proto`) plus options (priority, timeout).
**Output:** a stream of Arrow record batches for the final result, and query/fragment status updates.

```
            SubmitQuery / FetchResults
   Client  <---------------------------->  +-----------------------------+
                                           |          Scheduler          |
   SQL frontend -- physical plan --------> |  Plan Fragmenter            |
   (DataFusion)                            |  Query State Manager        |
                                           |  Task Queue + Policy        |
                                           |  Worker Manager             |
                                           |  Dispatcher / Result Gateway|
                                           +--------------+--------------+
                                      ExecuteFragment     |  Heartbeat / ReportTaskStatus
                        +-----------------+---------------+------------------+
                        v                 v                                  v
                   Worker 1  <----->  Worker 2  <---- Arrow Flight ---->  Worker N
                        \                 |                                  /
                         +------------- Parquet files (local / S3) ---------+
```

**Components**

1. **Plan Fragmenter.** Walks the plan tree and cuts it at exchange boundaries (repartition, coalesce, final merge). Each cut inserts a shuffle-write in the producer fragment and a remote-read in the consumer, annotated with the producer's address. Output is a DAG of fragments.
2. **Query State Manager.** Per-query state machine (`QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`). Tracks fragment dependencies and the ready set.
3. **Task Queue and Policy.** Holds ready fragments. Baseline is FIFO; later policies use priority, estimated cost, and queue wait time.
4. **Worker Manager.** Registry of nodes built from registration and heartbeats. Tracks free slots and liveness, and evicts nodes that miss heartbeats.
5. **Dispatcher.** Chooses a worker for each ready fragment (round-robin first, locality-aware later) and calls `ExecuteFragment`.
6. **Result Gateway.** Connects the root fragment's output stream to the client's `FetchResults`.
7. **Reference Worker.** A thin service that wraps DataFusion to execute fragments, implementing `ExecuteFragment` and the exchange operators. It keeps the project self-contained and testable end to end.

**Configuration knobs:** max concurrent queries, slots per worker, heartbeat interval and timeout, max fragment retries, scheduling policy, queue capacity.

## API Specification

**Client-facing (`SchedulerService`, gRPC)**

| RPC | Purpose |
|---|---|
| `SubmitQuery(plan, options) -> query_id` | Submit a serialized physical plan. Returns immediately. |
| `GetQueryStatus(query_id) -> status` | Query state plus per-fragment progress. |
| `FetchResults(query_id) -> stream<RecordBatch>` | Streams final results from the root fragment. |
| `CancelQuery(query_id)` | Stops all running fragments of the query. |

**Worker-facing (`WorkerRegistry`, gRPC)**

| RPC | Purpose |
|---|---|
| `RegisterWorker(address, resources)` | A worker announces itself and its capacity (slots, memory). |
| `Heartbeat(worker_id, load)` | Liveness and load reporting. |
| `ReportTaskStatus(task_id, status)` | Worker reports fragment completion or failure. |

**Scheduler-to-worker:** `ExecuteFragment(fragment, input_locations)` runs a fragment and tells it which nodes to pull child tuples from.

**Encoding:** Protobuf over gRPC for control; `datafusion-proto` for plans; Arrow IPC over Arrow Flight for data.

**Errors (gRPC status codes, with `query_id`, `fragment_id`, and reason in the details)**

| Situation | Code |
|---|---|
| Malformed or undeserializable plan | `INVALID_ARGUMENT` |
| Unknown `query_id` / `worker_id` | `NOT_FOUND` |
| No workers available or queue full | `RESOURCE_EXHAUSTED` |
| Fragment fails after max retries | `ABORTED` |
| Worker unreachable | `UNAVAILABLE` (retry on another node) |
| Client cancels | `CANCELLED` |

## Design Rationale

**Goals:** correct distributed execution, good utilization, resilience to worker failure, and a design that is easy to test in isolation.

* **Fragmenting at exchange operators.** Exchanges are natural stage boundaries. Each fragment stays a valid standalone plan (correctness) and reuses DataFusion's plan tree (low complexity).
* **Scheduler-push dispatch.** The scheduler assigns fragments to workers. This gives central control of placement, which locality-aware scheduling needs, and keeps state easy to reason about. A pull-based variant is a stretch goal for comparison.
* **gRPC and Arrow Flight.** Typed, well supported in Rust, and Flight avoids a second serialization format for data.
* **In-memory query state.** Simple and fast; queries are short-lived.
* **Wrapping DataFusion in the reference worker.** Avoids writing an execution engine and keeps the focus on scheduling.

**Alternatives considered**

* *Decentralized scheduling (workers coordinate among themselves):* correctness is hard to reason about and to test.
* *Scheduling the whole DAG up front:* cannot react to failures or load changes.
* *Persisting query state to disk:* out of scope for short-lived analytic queries.

**Prior art (for reference only):** existing distributed DataFusion schedulers such as Apache DataFusion Ballista. Study the ideas, write your own implementation.

## Testing Plan

**Unit tests**

* Fragmenter: golden tests from plan to fragment DAG for scans, filters, aggregates, joins, and sorts.
* Query state machine: legal and illegal transitions.
* Scheduling policies: given a queue and worker state, verify FIFO, priority, and locality choices.

**Integration and regression tests (through the public API)**

* Mock workers returning canned batches: submit, status, fetch end to end.
* Fault injection: kill a worker mid-fragment and verify retry elsewhere; drop heartbeats and verify eviction.
* Cancellation: cancel mid-query, verify all fragments stop and slots are freed.
* Correctness: run TPC-H (SF 1) on a multi-node setup and compare to single-node DataFusion.

**Performance experiments**

* Scaling: TPC-H runtime vs. worker count (1, 2, 4, 8).
* Scheduling overhead: submit-to-first-dispatch latency and throughput on many small concurrent queries.
* Policy comparison: FIFO vs. priority on a mixed short/long workload (mean and p99 latency).
* Baseline: single-node DataFusion.

## Trade-offs and Potential Problems

* **Single scheduler is a single point of failure.** No replication, accepted for scope.
* **Coupled to DataFusion's plan format.** Version upgrades may break serialization.
* **Lost intermediate data on worker failure.** Fragments whose outputs lived on a dead node must be recomputed, possibly cascading upstream.
* **In-memory state only.** A scheduler crash loses all running queries.
* **Exchange implementation effort.** Shuffle-write and remote-read operators may be the hardest part and could take longer than planned.

## Glossary

* **Fragment:** a subtree of the physical plan executed on one node.
* **Exchange:** an operator that moves data between fragments (shuffle, broadcast, merge).
* **Slot:** one unit of concurrent fragment capacity on a worker.
* **Shuffle-write / remote-read:** operators inserted at fragment boundaries to send and receive data.

## Plan and Resources

**Phases**

| Phase | Deliverable |
|---|---|
| 1 | API definitions, mock workers, plan fragmenter prototype |
| 2 | Milestone 1 complete (static workers, TPC-H subset) |
| 3 | Milestone 2 complete (concurrency, failures, priority policy, benchmarks) |
| 4 | Stretch goals, write-up |

**Resources**

* **Software:** Rust toolchain, Apache DataFusion, `tonic`, `arrow-flight`, `datafusion-proto`, `tokio`, Docker for local multi-node runs.
* **Hardware:** a laptop for development; a small AWS account (4 to 8 instances) for multi-node benchmarks.
* **Data and workloads:** TPC-H at SF 1 and SF 10 (Parquet), plus a synthetic mixed short/long workload.

**Open questions**

1. Build the exchange operators from scratch, or adapt existing DataFusion ones?
2. Push vs. pull: implement both for a comparison, or only push?
3. How much of the locality-aware stretch goal requires building a cache service?
