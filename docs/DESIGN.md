# Distributed Query Scheduler

**Author:** _Your Name_
**Stack:** Rust, Apache DataFusion, tonic (gRPC), Arrow Flight, tokio
**Headline goal:** near-linear scale-out speedup of analytical queries as workers are added.

A personal project to build a standalone query scheduler for a distributed, cloud-native OLAP engine built on Apache DataFusion, and to measure how well it scales out.

## Overview

The scheduler is a standalone service that turns a physical query plan into a parallel execution across many worker nodes. It receives an Apache DataFusion physical plan, splits it into fragments, expands each fragment into partition-level tasks, assigns tasks to workers as they have capacity, and streams the final result back to the client.

**Primary objective: scale-out speedup.** For a fixed dataset, query runtime should drop as workers are added. The project is judged by how close it gets to ideal scaling and by how well it can explain the gap.

* **Speedup:** S(N) = T(1) / T(N), where T(N) is query runtime on N workers.
* **Efficiency:** E(N) = S(N) / N. Ideal is 1.0.

A simple model guides the analysis:

> T(N) ≈ T_serial + T_parallel / N + T_overhead(N)

`T_serial` is work that cannot be spread out (planning, the final merge, result transfer to the client). `T_overhead(N)` grows with N (scheduling, shuffle fan-out, stragglers, skew). The project's job is to push `T_parallel` out to many workers while keeping the other two terms small, and to measure each term.

**What it does**

1. Breaks a DataFusion physical plan into fragments (stages) connected by data exchanges.
2. Expands each fragment into tasks, one per data partition, so a fragment runs on many workers at once.
3. Assigns tasks to workers dynamically to keep every worker busy.
4. Moves data between stages with a hash-partitioned shuffle.
5. Returns results to the client and reports a per-stage timing breakdown.

**Non-goals:** query optimization, writing a new execution engine (workers wrap DataFusion), transactions, security and authentication.

### Milestones

* **Milestone 1: correct end to end.** One task per fragment, a static set of workers, one query at a time. Results match single-node DataFusion on a TPC-H subset.
* **Milestone 2: it scales.** Partition-level task expansion, hash shuffle between stages, dynamic task assignment. Measurable speedup from 1 to 4 workers on a TPC-H subset.
* **Milestone 3: understand the scaling.** Full TPC-H at SF 10 on 1, 2, 4, and 8 workers. Per-stage timing breakdown, skew and straggler analysis, and fixes for the largest bottlenecks. Basic task retry on worker failure.
* **Stretch goals:** pipelined stages (start a stage before its inputs finish), locality-aware placement, concurrent queries with a priority policy, speculative re-execution of stragglers.

## Architectural Design

**Input:** a serialized DataFusion physical plan (via `datafusion-proto`) plus options.
**Output:** a stream of Arrow record batches for the final result, query and task status, and a timing breakdown.

```
            SubmitQuery / FetchResults
   Client  <---------------------------->  +-----------------------------+
                                           |          Scheduler          |
   SQL frontend -- physical plan --------> |  Plan Fragmenter            |
   (DataFusion)                            |  Task Expander              |
                                           |  Query State Manager        |
                                           |  Task Queue + Assigner      |
                                           |  Worker Manager             |
                                           |  Metrics / Timing           |
                                           +--------------+--------------+
                                         ExecuteTask      |  Heartbeat / ReportTaskStatus
                        +-----------------+---------------+------------------+
                        v                 v                                  v
                   Worker 1  <----->  Worker 2  <---- Arrow Flight ---->  Worker N
                        \                 |                                  /
                         +------------- Parquet files (local / S3) ---------+
```

### Components

1. **Plan Fragmenter.** Walks the plan tree and cuts it at exchange boundaries. Each cut inserts a shuffle-write in the producer fragment and a remote-read in the consumer. Output is a DAG of fragments. Fragment ids are assigned in post-order, so dependencies always have smaller ids. (Prototype built.)
2. **Task Expander (new).** Turns each fragment into tasks, one per partition. A task is the unit of scheduling: `(query_id, fragment_id, partition)`.
   * *Scan fragments:* split by Parquet file or row-group range. Many small splits balance load, in the spirit of morsel-driven parallelism.
   * *Fragments reading a hash exchange with P partitions:* P tasks. Task `j` pulls bucket `j` from every producer task.
   * *Fragments reading a coalesce exchange:* one task.
3. **Query State Manager.** Per-query state machine plus per-task state (`PENDING`, `READY`, `RUNNING`, `SUCCEEDED`, `FAILED`).
4. **Task Queue and Assigner.** Holds ready tasks and assigns them to workers with free slots, always to the least-loaded worker. Assignment is dynamic, so a worker that finishes early takes more work and fast and slow tasks even out.
5. **Worker Manager.** Registry of workers from registration and heartbeats. Tracks slots and liveness, and evicts workers that miss heartbeats.
6. **Metrics and Timing (new).** Records per-task start and end, bytes shuffled, queue wait, and worker utilization. This is the data behind the scaling analysis.
7. **Reference Worker.** A thin service that wraps DataFusion to execute tasks, writing hash-partitioned output for downstream tasks and reading its inputs from other workers over Arrow Flight.

### Worked example: tasks for one query

> This table uses a simplified plan shape. The real DataFusion 55.1 plan for the same query has 6 fragments and 15 tasks (with one scan split per table); see the Progress Log, "Phase 2b-2, slice 1 findings".

For the join, aggregate, and sort query used in the design walkthrough, with 4 hash partitions:

| Fragment | Work | Tasks | Why |
|---|---|---|---|
| F0 | Filter + scan `orders` | 8 | one per Parquet row-group range |
| F1 | Scan `customers` | 2 | one per file |
| F2 | Join + partial aggregate | 4 | one per hash partition of the join key |
| F3 | Final aggregate | 4 | one per hash partition of the group key |
| F4 | Global sort (root) | 1 | merge to a single node |

F4 and the client transfer are `T_serial`. Everything else can spread across workers, which is where speedup comes from.

**Configuration knobs:** partitions per shuffle, scan split size, slots per worker, max concurrent queries, heartbeat interval and timeout, max task retries.

## API Specification

**Client-facing (`SchedulerService`, gRPC)**

| RPC | Purpose |
|---|---|
| `SubmitQuery(plan, options) -> query_id` | Submit a serialized physical plan. Returns immediately. |
| `GetQueryStatus(query_id) -> status` | Query state plus per-task progress. |
| `GetQueryMetrics(query_id) -> metrics` | Per-stage timing and shuffle statistics (new). |
| `FetchResults(query_id) -> stream<RecordBatch>` | Streams final results from the root fragment. |
| `CancelQuery(query_id)` | Stops all running tasks of the query. |

**Worker-facing (`WorkerRegistry`, gRPC)**

| RPC | Purpose |
|---|---|
| `RegisterWorker(address, resources)` | A worker announces itself and its slots. |
| `Heartbeat(worker_id, free_slots)` | Liveness and capacity reporting. |
| `ReportTaskStatus(task_id, status, metrics)` | Worker reports completion, failure, and timing. |

**Worker data plane:** `FetchTaskOutput(task, bucket) -> stream<OutputChunk>` streams one bucket of a finished task's output. The scheduler calls it to fetch final results, and a downstream task calls it on each producer to pull its shuffle input. One RPC serves both, which keeps the exchange layer small. Chunks are opaque bytes for now; Arrow Flight is a candidate replacement.

**Scheduler-to-worker:** `ExecuteTask(task)` replaces `ExecuteFragment`. A task carries the fragment plan, its partition index, the number of output partitions, and for each input fragment the list of producer tasks and their worker addresses so the task knows where to pull from.

**Planned proto changes from the Phase 1 skeleton:** add `task_id` and `partition` to the execute request, replace the flat `InputLocation` with per-partition producer locations, and add `GetQueryMetrics`.

**Encoding:** Protobuf over gRPC for control; `datafusion-proto` for plans; Arrow IPC over Arrow Flight for data.

**Errors (gRPC status codes, with `query_id`, `task_id`, and reason in the details)**

| Situation | Code |
|---|---|
| Malformed or undeserializable plan | `INVALID_ARGUMENT` |
| Unknown `query_id` / `worker_id` | `NOT_FOUND` |
| No workers available or queue full | `RESOURCE_EXHAUSTED` |
| Task fails after max retries | `ABORTED` |
| Worker unreachable | `UNAVAILABLE` (retry on another worker) |
| Client cancels | `CANCELLED` |

## Design Rationale

**Goals:** high parallel efficiency, balanced load across workers, low scheduling overhead, and a design whose scaling behavior can be measured and explained.

* **Partition-level tasks.** Fragment-level scheduling puts a whole stage on one worker, so adding workers does nothing. Tasks per partition are what let one stage run on N workers.
* **Many small tasks.** More tasks than slots lets dynamic assignment smooth out uneven task durations. The cost is per-task overhead, so task size is a tuning knob, evaluated in the benchmarks.
* **Dynamic, least-loaded assignment.** Static assignment (round-robin up front) suffers when tasks differ in duration. Assigning on demand keeps workers busy.
* **Hash shuffle over Arrow Flight.** Task `j` of a downstream stage reads bucket `j` from every upstream task. One format end to end, no extra serialization.
* **Strict stage barriers first.** A stage starts only when its inputs are fully done. This is simple to reason about and makes correctness easy to verify. Pipelining is a stretch goal.
* **Wrapping DataFusion in the reference worker.** Keeps the project focused on scheduling and exchange rather than on writing operators.

**Alternatives considered**

* *Fragment-level scheduling only:* simpler, but gives almost no speedup.
* *Static partition-to-worker assignment:* less scheduler work, but poor load balance under skew.
* *Pull-based task fetching by workers:* a reasonable alternative. A comparison with push-style assignment is a stretch experiment.
* *Decentralized scheduling:* correctness is hard to reason about and to test.

**Prior art (for reference only):** existing distributed DataFusion schedulers such as Apache DataFusion Ballista. Study the ideas, write your own implementation.

## Evaluation Plan

### Correctness tests

* **Fragmenter and expander (unit):** golden tests from plan to fragment DAG to task list, including the worked example above.
* **State machines (unit):** legal and illegal transitions for queries and tasks.
* **Assigner (unit):** given a task queue and worker capacity, verify least-loaded selection.
* **Mock workers (integration):** end-to-end submit, status, fetch with canned data.
* **Result equivalence:** every TPC-H query's result on N workers equals single-node DataFusion's result.
* **Fault injection:** kill a worker mid-task and verify retry; drop heartbeats and verify eviction; cancel mid-query and verify slots are freed.

### Scaling experiments (the main deliverable)

Run on separate machines with a fixed size (for example cloud instances with the same vCPU count). Do not benchmark scale-out on one laptop with many processes, because they share the same CPU and memory and the results will mislead.

* **Strong scaling:** fixed data (TPC-H SF 10), workers = 1, 2, 4, 8. Report runtime, speedup S(N), and efficiency E(N) per query and overall.
* **Weak scaling:** data size grows with workers (for example SF 2.5 / 5 / 10 / 20 on 1 / 2 / 4 / 8). Ideal runtime stays flat.
* **Per-stage breakdown:** time per fragment, to show where speedup stalls (scan scales, final merge does not).
* **Overhead measurements:** scheduling latency per task, shuffle bytes and time, worker utilization over time, and queue wait.
* **Tuning sweeps:** partitions per shuffle and scan split size vs. runtime.
* **Skew study:** a skewed key distribution, to show imbalance and test a mitigation.
* **Baselines:** single-node DataFusion on the same instance type, and the ideal 1/N curve.

## Trade-offs and Potential Problems

* **Scale-out is limited by the serial part.** The final merge and result transfer don't shrink with more workers (Amdahl's law). The analysis should quantify this, not hide it.
* **Shuffle cost grows with N.** Each downstream task pulls from every upstream task, so connections and small transfers multiply. Efficiency will likely drop at 8 workers.
* **Data skew.** One hot partition makes one task the straggler for its whole stage.
* **Stage barriers waste time.** Idle workers wait for the slowest task. Pipelining would help but adds complexity.
* **Single scheduler is a single point of failure and a possible bottleneck** at high task counts. No replication.
* **Coupled to DataFusion's plan format.** Version upgrades may break serialization.
* **Lost intermediate data on worker failure.** Tasks whose outputs lived on a dead node must be recomputed, possibly cascading upstream.
* **Benchmark cost.** Multi-machine runs need a cloud budget. Keep correctness testing local and run scaling experiments in batches.

## Glossary

* **Fragment:** a subtree of the physical plan between exchanges.
* **Task:** one partition of one fragment, the unit of scheduling.
* **Exchange:** an operator that moves data between fragments (hash shuffle, broadcast, merge).
* **Slot:** one unit of concurrent task capacity on a worker.
* **Speedup / efficiency:** S(N) = T(1)/T(N); E(N) = S(N)/N.
* **Strong / weak scaling:** fixed total data vs. data growing in proportion to workers.
* **Straggler:** a task that runs much longer than its peers and delays its stage.

## Plan and Resources

**Phases**

| Phase | Deliverable |
|---|---|
| 1 | API definitions, mock workers, plan fragmenter prototype, skeleton service (done) |
| 2a | Task dispatch end to end with mock workers (done): JSON plan input, slot-aware assignment, completion, failure, cancel |
| 2a.1 | Automated end-to-end tests (done) |
| 2b-1 | Worker output service and `FetchResults` (done) |
| 2b-2 | Milestone 1: DataFusion adapter and real worker execution (next) |
| 3 | Milestone 2: task expander, hash shuffle, dynamic assignment, first scaling numbers |
| 4 | Milestone 3: full TPC-H scaling study, bottleneck analysis, skew and retry |
| 5 | Stretch goals and write-up |

**Resources**

* **Software:** Rust toolchain, Apache DataFusion, `tonic`, `arrow-flight`, `datafusion-proto`, `tokio`, Docker for local correctness runs.
* **Hardware:** a laptop for development; 8 identical cloud instances for scaling experiments.
* **Data and workloads:** TPC-H at SF 1 (development) and SF 10+ (benchmarks) as Parquet, plus a skewed synthetic dataset.

## Background Reading

* **Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age** (Leis et al., SIGMOD 2014). Take: split work into many small units that workers pull dynamically, for load balance.
* **Dremel: A Decade of Interactive SQL Analysis at Web Scale** (Melnik et al., VLDB 2020). Take: how a production distributed engine shuffles between stages.
* **Building an Elastic Query Engine on Disaggregated Storage** (Vuppalapati et al., NSDI 2022). Take: how scaling compute interacts with storage and data movement.
* **The Snowflake Elastic Data Warehouse** (Dageville et al., SIGMOD 2016). Take: elastic compute over shared storage, the architecture this project targets.
* **Self-Tuning Query Scheduling for Analytical Workloads** (Wagner et al., SIGMOD 2021). Take: ideas for smarter scheduling policies, relevant to the stretch goals.
* **To Partition, or Not to Partition, That Is the Join Question in a Real System** (Bandle et al., SIGMOD 2021). Take: when partitioning for joins pays off, which informs the choice of shuffle partition count.

**Open questions**

1. Build the exchange operators from scratch, or adapt existing DataFusion ones?
2. How many partitions per shuffle by default, and should it scale with worker count?
3. When to add pipelined stages: only if the benchmark shows barrier idle time dominates?

## Progress Log

A running record of what was built, the decisions behind it, and what is known to be missing.

### Phase 1: skeleton (done)

* gRPC API in `proto/scheduler.proto`, plus a scheduler service, a worker registry with heartbeats and stale-worker eviction, and a query state machine.
* `fragmenter` crate: cuts a plan at exchange boundaries into a fragment DAG (post-order ids, so dependencies always have smaller ids).
* `mock-worker`: registers, heartbeats, and fakes task execution.
* Verification: unit tests for the fragmenter, worker registry, and query state machine.

### Task model (done)

* `fragmenter::tasks`: expands each fragment into per-partition tasks. This is what lets one stage run on many workers, which scale-out speedup depends on.
* Task count per fragment: a hash or round-robin input gives `shuffle_partitions` tasks; otherwise scans give one task per split; otherwise one task.
* Proto: `ExecuteFragment` became `ExecuteTask`, with `TaskId`, per-producer input locations (`ProducerLocation`, including the bucket to read), per-task metrics, and `GetQueryMetrics`.
* Verification: golden tests for the worked example (5 fragments become 8, 2, 4, 4, 1 tasks), bucket assignment, stage-barrier readiness, a broadcast-join case, and config clamping.

### Phase 2a: dispatch with mock workers (done)

What was built

* `execution.rs`: per-query state (task phases, where each finished task's output lives, failure and cancel flags). Pure logic with no I/O.
* `engine.rs`: `submit` (decode, fragment, expand, register), `on_task_report`, `cancel`, `handle_evicted_workers`, and a background dispatcher loop.
* `workers.rs`: slot accounting. `acquire` picks the least-loaded worker (lowest in-flight over slots); `release` frees a slot.
* Query lifecycle: QUEUED, then RUNNING at first dispatch, then SUCCEEDED when every task is done, or FAILED if any task fails. `CancelQuery` frees in-flight slots and stops further dispatch.
* `GetQueryStatus` now reports per-task state and worker.
* `examples/join_agg.json`: the worked-example plan, used by a test and by the README demo.

Decisions

* **Interim plan format:** the JSON encoding of the fragmenter's own plan type. A DataFusion adapter replaces it in Phase 2b. Reason: the adapter depends on version-specific DataFusion APIs and should be built where it can be compiled and iterated on.
* **Scheduler owns slot accounting.** Workers' heartbeat-reported free slots are ignored, because two sources of truth drift. Heartbeats only prove liveness.
* **One dispatcher loop, woken by events plus a 200 ms timer.** Simpler than per-query tasks, and the timer retries work that was waiting for capacity.
* **Strict stage barrier:** a task is dispatched only after every task of its input fragments is done.
* **Failure policy for now:** any failed task fails the query. In-flight tasks of an evicted worker are re-queued.
* **No lock held across a network call:** state is reserved and the request built under the lock, then the lock is released before dispatching.

Known gaps (to address later)

* `FetchResults` was unimplemented at this point (resolved in Phase 2b-1).
* Outputs of finished tasks on a lost worker are not recomputed, so downstream tasks that need them will fail. Only in-flight tasks are re-queued.
* No retries on task failure (planned for Milestone 3).
* A new connection is opened per dispatched task; channels should be reused.
* `CancelQuery` does not tell workers to stop running tasks.
* Per-table scan splits come from one global setting (`SCHEDULER_SCAN_SPLITS`) rather than table metadata.
* Task metrics are defined in the API but not yet collected.

Verification

* Unit tests: slot acquire/release and load spreading; execution state (stage barrier, input locations, duplicate reports, failure, cancel, requeue, revert); engine flows (invalid plans, example plan expands to 11 tasks, report completes or fails a query, report frees a slot, cancel).
* End-to-end: run the scheduler with two mock workers and submit `examples/join_agg.json` (see the README). Expect the query to go QUEUED, RUNNING, SUCCEEDED, with tasks spread across both workers.
* Not covered by tests: the async dispatch path over real gRPC. It is exercised only by the end-to-end run.

### Phase 2a.1: automated end-to-end tests (done)

Why: until now the only end-to-end check was the manual README run, so a regression in the gRPC dispatch path would have gone unnoticed.

* The `scheduler` crate is now a library plus a thin binary, with `scheduler::serve(addr, engine)`. Tests can start a real scheduler in-process.
* `crates/scheduler/tests/e2e.rs` starts a scheduler and gRPC test workers on local ports and drives them through the real API:
  * a query runs across two workers, all 11 tasks succeed, both workers receive work, and every task arrives with the right input locations, buckets, and output layout (which also proves the stage barrier held);
  * a failed task fails the query with its message, and results of a failed query are refused;
  * a query submitted with no workers stays queued, then completes once a worker registers;
  * invalid plans are rejected with `INVALID_ARGUMENT`, and results of unknown queries return `NOT_FOUND`.
* Decision: the test workers live in the test file rather than reusing the `mock-worker` binary, so tests can record what the scheduler sends and inject failures.
* Known weakness: ports are chosen by binding and releasing a free port, which has a small race. The helper never reuses a port within one test run.

### Phase 2b-1: worker output service and FetchResults (done)

* API: new `WorkerService.FetchTaskOutput(task, bucket)` stream. It is the data-plane read used for both final results and, later, shuffle input.
* Scheduler: `FetchResults` now works. It requires a SUCCEEDED query, finds the root-fragment tasks and the workers holding their output, and streams their output to the client in partition order.
* Mock worker: serves a canned payload that names the task and bucket, so tests and demos can tell where a result came from.
* Decisions: results are opaque bytes for now; `FetchResults` streams only after the query succeeds (no early streaming); the root fragment's output is bucket 0 of each root task.
* Known gaps: no Arrow decoding or schema handling yet; if a worker holding a root task's output dies before results are fetched, `FetchResults` fails (no recompute); results are not cleaned up on workers.
* Verification: unit tests for root-output lookup and result sources, plus the end-to-end test above that fetches the result stream.

### Phase 2b-2, slice 1: DataFusion version spike (added; output pending)

* New crate `crates/df-spike`. It writes two small Parquet files, runs the worked-example query with 4 target partitions, prints the real physical plan, the operator tree, and the distinct operator names, then round-trips the plan through `datafusion-proto` and executes the decoded plan.
* **Decision: pin DataFusion and `datafusion-proto` to exactly 55.1.0** (`=55.1.0`), the latest release at the time. Reasons: the `datafusion-proto` docs state that serialized plans are not guaranteed to be compatible across DataFusion versions, and physical-plan APIs change between releases.
* API facts taken from the 55.1 docs: plans are serialized with `datafusion_proto::bytes::physical_plan_to_bytes` and decoded with `physical_plan_from_bytes(&bytes, &ctx.task_ctx())`; Parquet scan serialization is part of the default `parquet` feature.
* Dependency finding: `datafusion-proto` 55.1.0 depends on `prost` 0.14 and `arrow` 59, while `scheduler-proto` uses `tonic` 0.12 and `prost` 0.13. They can coexist as separate crates as long as their types are not shared. When Arrow Flight is added, expect to upgrade `tonic` and `prost` to versions that match the Arrow release (to be checked then).
* Build note: `df-spike` is excluded from `default-members` so `cargo build` and `cargo test` stay fast; CI builds the whole workspace.
* Not yet verified: the spike was written from the 55.1 docs without compiling, and `generate_series` column naming, the `COPY ... TO ... STORED AS PARQUET` single-file behavior, and the join-mode settings may need adjusting.
* What to look for in its output: which operators appear at exchange boundaries (expected: repartition, coalesce, and sorted-merge operators), whether the join is partitioned, and whether the decoded plan executes.

### Phase 2b-2, slice 1 findings: the real plan (spike output received)

The spike ran on the first attempt: the plan serialized with `datafusion-proto`, decoded, and executed (3 result rows), and the debug output was identical after the round trip. The real plan for the worked-example query (4 target partitions, partitioned joins):

```
SortPreservingMergeExec: [revenue DESC]
  ProjectionExec
    SortExec: preserve_partitioning=[true]
      AggregateExec: mode=FinalPartitioned, gby=[segment]
        RepartitionExec: Hash([segment], 4), input_partitions=4
          AggregateExec: mode=Partial, gby=[segment]
            HashJoinExec: mode=Partitioned, on=[(id, cust_id)]
              RepartitionExec: Hash([id], 4), input_partitions=1
                DataSourceExec: customers.parquet
              RepartitionExec: Hash([cust_id], 4), input_partitions=4
                FilterExec: order_date >= 2024-01-01
                  RepartitionExec: RoundRobinBatch(4), input_partitions=1
                    DataSourceExec: orders.parquet
```

What it taught us, and decisions

1. **Exchange operators.** `RepartitionExec` (hash and round-robin) and `SortPreservingMergeExec` mark the boundaries. `CoalescePartitionsExec` is expected in other plans.
2. **Cut rules** (implemented in `df-adapter`):
   * `RepartitionExec`: the node is replaced by a shuffle reader; the subtree below becomes a producer fragment that writes buckets.
   * `SortPreservingMergeExec` and `CoalescePartitionsExec`: the operator stays in the consumer fragment and its child is cut, so the consumer reads each producer task's output as a separate partition and merges.
3. **A task is `(fragment, partition)`:** it runs the fragment's plan for one partition index. A fragment has as many tasks as its root operator has output partitions. This avoids rewriting scans per task.
4. **The real plan is 6 fragments and 15 tasks** (1, 1, 4, 4, 4, 1), not the 5 fragments and 11 tasks of the simplified example.
5. **The sort is distributed.** Each of the 4 partitions sorts locally and the root merges the sorted streams, which scales better than the single global sort assumed earlier.
6. **The round-robin repartition on the orders scan exists only because the single file gave one scan partition.** With several scan partitions (file or row-group splits) it disappears, so scan splitting directly sets how parallel the scan stage is.
7. **Dynamic filters are present.** The orders scan carries a `DynamicFilter` fed by the join. That sharing works inside one process, but across fragments the scan never receives updates, so it would only lose the optimization, not return wrong results. The spike now prints the dynamic-filter settings so we can choose the setting to disable. Decision pending that output.
8. **`DataSourceExec` embeds absolute file paths.** Every worker must be able to read the same paths, through a shared filesystem, identical local copies, or an object store.
9. **API facts for DataFusion 55.1:** `as_any()` was removed from `ExecutionPlan` in 54; use `plan.downcast_ref::<T>()` and `plan.is::<T>()` directly on `Arc<dyn ExecutionPlan>`. `properties()` returns `&Arc<PlanProperties>`; read partitioning through `ExecutionPlanProperties::output_partitioning()`.

### Phase 2b-2, slice 2a: plan cutter (added; not yet run)

* New crate `crates/df-adapter`: `cut(plan) -> CutPlan` analyzes a real physical plan and reports fragments, their operators, inputs, output exchange, and task counts. It does not rewrite the plan yet.
* `df-adapter::example` holds the example query, sample-data generation, and a session configured like the spike, shared by tests and demos.
* Tests (run `cargo test -p df-adapter`): the example query must cut into exactly the 6 fragments above, with the expected operators, inputs, exchanges, and task counts (1, 1, 4, 4, 4, 1, total 15); and structural invariants (post-order ids, inputs precede consumers, only the root is delivered to the client) hold for three queries.
* `df-spike` now also prints the cut and the dynamic-filter settings.
* Not yet verified: written from the DataFusion 54/55 migration notes without compiling. The exact operator names and the plan shape in the test come from the spike output, but the test depends on the same session settings and data as the spike.

### Phase 2b-2, slice 2b-i: shuffle operators and a local distributed run (added; not yet run)

What was built (in `crates/df-adapter`)

* `store.rs`: an in-memory `ShuffleStore` keyed by (query, fragment, task, bucket), plus Arrow IPC encode and decode helpers. Empty buckets are stored too, so a reader can tell "empty" from "producer never ran".
* `shuffle.rs`: `ShuffleWriteExec` (runs one input partition, splits it into buckets with DataFusion's own `BatchPartitioner`, stores them, yields no rows) and `ShuffleReadExec` (a leaf that reads buckets back). Two read modes: `Bucket` (hash and round-robin: output partition `j` reads bucket `j` of every producer) and `PerProducer` (merge and coalesce: output partition `p` reads producer `p`).
* `rewrite.rs`: `distribute(plan, query_id, store)` rebuilds a real plan into fragment plans, replacing each exchange. Fragment order matches the cutter exactly. `run_locally` runs every task of every fragment one after another in one process and returns the root output.
* Tests compare `run_locally` with plain single-node DataFusion on three queries (the example query, a grouped aggregate, and a filtered sorted scan) and require identical output, check that fragment descriptions match the cutter, and check that reading a bucket before its producer ran is an error. Each distributed run has a 60 second timeout so a hang fails the test instead of freezing it.

Decisions and findings

1. **Dynamic filters must be off.** With dynamic filter pushdown on, a partitioned hash join makes each partition wait for all other partitions to report their build side. When partitions run as separate tasks or on separate workers, that wait never ends, so the query would hang, not just run slower. The master setting is `datafusion.optimizer.enable_dynamic_filter_pushdown` (it overrides the join, top-k, and aggregate switches); the example session and the spike now set it to false. Every session that plans a distributed query must do the same. (Names verified in the DataFusion 54 and later config docs.)
2. **Readers copy the properties of the node they replace** (partitioning, ordering, equivalences), so the operators above a cut see exactly what they saw before. Sharing the same properties object also lets DataFusion skip recomputing them.
3. **Hash partitioning reuses DataFusion's `BatchPartitioner`.** Rows land where `RepartitionExec` would put them, and both sides of a join agree. API note: in recent DataFusion `BatchPartitioner::try_new` takes the input partition and input partition count, and there are separate hash and round-robin constructors (verified in the 54.0.0 source).
4. **DataFusion 55 API changes hit on first compile.** The first build against the pinned 55.1.0 failed because `ExecutionPlan::apply_expressions` became a required method in 55, and `with_new_children` was deprecated in favor of `replace_children(children, ReplaceChildrenOptions)` (modes `Keep` and `Recompute` for plan properties). Lesson: reading the 54.0.0 source was not enough; check the 55.x upgrade guide for every trait we implement. Fixes: both shuffle operators implement `apply_expressions` (the writer visits its hash-key expressions, the reader owns none), and the rewriter calls `replace_children` with `Recompute`.
5. **The store is read in-process for now.** The worker will serve the same buckets over `FetchTaskOutput`; the IPC helpers are the encoding for that.

Not yet done

* Serializing the two operators (a `PhysicalExtensionCodec`) so plans can be sent to workers.
* Fetching buckets over the network instead of the in-process store.
* Memory limits and spilling for buckets.
* Written first against the 54.0.0 sources, then corrected for 55.x after the first compile (see decision 4); any further signature differences will show up as compile errors.

### Phase 2b-2, slice 2b-ii-a: plan serialization (added; not yet run)

What was built (`crates/df-adapter/src/codec.rs`)

* `ShuffleCodec` is a `PhysicalExtensionCodec`, so a whole fragment (standard operators plus `ShuffleWriteExec` and `ShuffleReadExec`) can be serialized with `datafusion-proto`, sent to a worker, and rebuilt. `encode_plan` and `decode_plan` wrap the byte functions.
* Wire format of an extension node: one tag byte (1 = write, 2 = read) followed by a small protobuf message defined in the crate with `prost` (no `.proto` file or `protoc` needed). Children are serialized by DataFusion and handed back to the codec on decode.
* Tests serialize every fragment of three queries, decode each into a new plan, run the decoded fragments, and require output identical to single-node DataFusion. A separate test checks the schema round trip.

Decisions

1. **Partitioning is serialized through a throwaway `RepartitionExec`.** A partitioning holds physical expressions (the hash keys). Instead of writing expression serialization by hand, the partitioning is wrapped in a `RepartitionExec` over an `EmptyExec`, serialized with DataFusion's own plan serialization, and unwrapped on decode. This works for any expression DataFusion can serialize. It is a deliberate shortcut: if DataFusion ever stops allowing a `RepartitionExec` over an `EmptyExec`, replace it with direct expression serialization.
2. **The decode side supplies the store.** The codec holds a handle to the `ShuffleStore`, which decoded operators read from and write to. The network version will replace the store handle with a bucket fetcher.
3. **Schemas travel as Arrow IPC** (a stream with no batches), reusing the helper that is already tested.
4. **Known loss:** a decoded `ShuffleReadExec` keeps its schema and partitioning but not its ordering metadata. Execution does not depend on it; the merge operator above a reader carries its own sort expressions.
5. **API facts (55.x):** `PhysicalExtensionCodec::try_decode` takes `(buf, inputs, task_ctx, proto_converter)` and `try_encode` takes `(node, buf, proto_converter)`; `physical_plan_to_bytes_with_extension_codec(plan, codec)` and `physical_plan_from_bytes_with_extension_codec(bytes, task_ctx, codec)` are the entry points. These were read from the 55.x docs, not from memory.

Not yet done: fetching buckets over the network (slice 2b-ii-b), and passing producer locations into decoded readers.

### Phase 2b-2, slice 2b-ii-b: network reads (added; not yet run)

Why: the in-process store is invisible across machines. A consumer on one worker must be able to fetch a bucket that a producer wrote on another. (This is also recorded in `QA.md`.)

What was built (`crates/df-adapter`)

* `BucketSource` (in `store.rs`): the interface a reader uses to get a bucket. `ShuffleStore` implements it for local reads. `ShuffleReadExec` now holds a `BucketSource` instead of a fixed store, and fetches when its stream is first polled, so a fetch can be asynchronous.
* `net.rs`:
  * `BucketServer` implements the worker side of `FetchTaskOutput` over a store. Each `OutputChunk` holds one batch as a complete Arrow IPC stream; an empty bucket sends no chunks. It counts requests so tests can prove data crossed the network.
  * `RoutedSource` knows which worker ran each producer task `(fragment, task) -> address`. Buckets on this worker are read from the local store; others are fetched from the owning worker over gRPC.
* `ShuffleCodec::with_source` lets the decoding side choose where readers fetch from (writers still write to the local store).
* The key test: two simulated workers, each with its own store and its own bucket server. Every task is decoded from bytes on the worker that runs it, so buckets produced on the other worker must cross gRPC. Results must equal plain single-node DataFusion, and the test also checks that both workers stored output and that fetches really happened. Two queries are covered.

Decisions

1. **Fetch from the producer, not a central store.** Data crosses the network once and nothing funnels through one machine, which matters for the scale-out measurements.
2. **Local shortcut:** a bucket whose producer ran on this worker is read from memory, with no network.
3. **One batch per message, one new connection per fetch.** Simple first; revisit after measuring.

Known limits

* A batch larger than tonic's default 4 MB message limit would fail; the limit can be raised or batches split.
* The scheduler does not yet send producer locations to a real worker; the test builds the location map by hand. Wiring that into `ExecuteTask` is part of slice 3.
* No retries, timeouts, or cleanup of old buckets.
* New dependency edge: `df-adapter` now depends on `scheduler-proto` (tonic 0.12 / prost 0.13) alongside DataFusion (prost 0.14). They coexist as long as their types are not shared.

### Next steps (drafted plan)

Status when this was written: the spike ran, and the plan cutter (`df-adapter`, slice 2a) is added but not yet run. The remaining work is in three slices, then the benchmarking phase. Each slice has a verification step so problems surface early.

#### Slice 2b: shuffle nodes (make a fragment a valid, serializable DataFusion plan)

Progress: the operators, the rewriter, and the local run (slice 2b-i), plan serialization with a codec (slice 2b-ii-a), and network reads (slice 2b-ii-b) are all added. Slice 2b is complete once the last two are verified; the real worker is slice 3.

Goal: turn the cutter's analysis into real plans. Each fragment becomes a plan a worker can decode and run for one partition.

* **`ShuffleWriteExec`** wraps a producer fragment's root. For task partition `i` it runs the fragment for input partition `i`, splits the output into N buckets (hash on the exchange keys, round-robin, or a single bucket for merge and coalesce), and stores the buckets in the worker's output store. Its own output stream is empty.
* **`ShuffleReadExec`** is a leaf in the consumer fragment. For consumer partition `j` it pulls bucket `j` from every producer task through `FetchTaskOutput` and decodes the Arrow IPC bytes into a record-batch stream. For merge and coalesce exchanges, each producer task is its own partition, so `SortPreservingMergeExec` can merge sorted streams.
* **Serialization:** implement a `PhysicalExtensionCodec` that encodes both nodes (schema, exchange kind, input fragment id, producer count), so plans travel in `ExecuteTaskRequest.fragment_plan`.
* **Producer locations** are not known when the plan is built. The scheduler sends them in `ExecuteTaskRequest.inputs`, and the worker attaches them to the `ShuffleReadExec` nodes after decoding, keyed by input fragment id.
* **Verification (the key test):** an in-process "mini scheduler" runs all 15 tasks of the example query one after another through an in-memory shuffle and compares the result with plain single-node DataFusion. Also test the codec round trip and write-then-read of each exchange kind. None of this needs a network.
* **Risks:** custom `ExecutionPlan` nodes depend on the 55.1 trait (plan properties, `with_new_children`, `execute`), which changes between releases, so each API use is checked against the docs. Ordering metadata for the merge exchange must be preserved.

#### Slice 3: real worker and scheduler integration

Goal: replace the mock worker and the interim JSON plan format, so a real query runs across real workers.

* **`worker` crate** implements `WorkerService`: `ExecuteTask` decodes the plan with the codec, wires up producer locations, runs the task's partition, and stores the output buckets; `FetchTaskOutput` serves buckets from the store. It registers, heartbeats, and reports status with metrics (start and end time, rows, bytes), which also fills `GetQueryMetrics`.
* **Output store:** in memory first, spilling to disk later; buckets are removed when the query finishes (needs a cleanup RPC or a time limit).
* **Scheduler integration:** `SubmitQuery` accepts a `datafusion-proto` physical plan. To keep the scheduler free of a DataFusion dependency, introduce a neutral `FragmentGraph` (fragments, inputs, exchange kinds, task counts, plan bytes per fragment). `df-adapter` produces it and the scheduler consumes it, replacing the JSON plan model for real queries.
* **Client tool:** a small command-line client that registers Parquet tables, plans a SQL query with DataFusion, cuts it, submits it, and prints the result. The benchmark runner will use it.
* **Data access:** plans embed file paths, so every worker needs the same data at the same path. Use identical local copies or an object store.
* **Verification:** an end-to-end test with real workers in-process checks that the example query's result equals single-node DataFusion; then a TPC-H subset.
* **Milestone 1 (correct end to end) is reached after this slice.**

#### Slice 4: correctness at scale

* Generate TPC-H data as Parquet at SF 1 for development and SF 10 for benchmarks, using a TPC-H generator (for example `tpchgen-rs`; availability to be checked).
* Run the TPC-H queries on N workers and compare every result with single-node DataFusion. Record which queries hit unsupported operators or cut cases.
* Decide scan split sizes so scan stages are parallel (this removes the single-partition round-robin seen in the spike plan).

#### Phase 4: the scale-out study (the headline goal)

* Infrastructure: 8 identical cloud instances, the same data on each (or an object store), a runner script, and metric collection from `GetQueryMetrics`.
* Strong scaling at 1, 2, 4, 8 workers on SF 10; weak scaling with data growing in proportion to workers.
* Per-stage time breakdown, shuffle volume, scheduling latency, and worker utilization; tuning sweeps over shuffle partitions and scan split size; a skew experiment.
* Compare against single-node DataFusion on the same instance type and against the ideal 1/N curve; report speedup and efficiency, and explain the gaps with the T_serial / T_parallel / T_overhead model.
* Milestones 2 and 3 are reached here.

#### Decisions still open

1. ~~Dynamic filters: which setting disables them.~~ Resolved: set `enable_dynamic_filter_pushdown` to false (required, see slice 2b-i).
2. Neutral `FragmentGraph` vs. converting the cutter's output to the existing fragmenter types (leaning neutral graph).
3. Keep gRPC byte streaming for the data plane, or move to Arrow Flight (needs a `tonic` and `prost` upgrade); decide after measuring shuffle cost.
4. Output cleanup policy on workers.
5. How workers get the data (identical local copies, shared filesystem, or object store).
