# Clarifying Questions and Answers

A running log of questions asked while building this project, with short answers. It is
meant as a learning record: when something is unclear, the question and answer go here.
Longer explanations live in `DESIGN.md`.

## Tools and concepts

**What is Cargo?**
Rust's build tool and package manager. `cargo build` compiles, `cargo test` builds and runs
tests, and `cargo run -p <crate>` runs one part of the project. It downloads libraries listed
in each `Cargo.toml` from crates.io. This repository is a Cargo workspace: several related
packages (crates) in one repo. `Cargo.lock` pins exact dependency versions and is committed;
`target/` holds build output and is not.

**What is DataFusion?**
An open-source query engine written in Rust. It parses and plans SQL, optimizes the plan, and
runs it on one machine, in parallel across that machine's cores, using Apache Arrow in memory.
It has no server, storage layer, or distribution. In this project each worker wraps DataFusion,
and the scheduler adds the distribution: it cuts a plan into pieces, sends them to workers,
and moves data between them.

**What is the "spike" (`crates/df-spike`)?**
A small throwaway experiment built to learn something unknown before designing around it.
Ours wrote sample Parquet files, ran the example query through real DataFusion, printed the
real physical plan, and checked that a plan can be serialized, decoded, and run again. It
replaced guesses about plan shape with facts (6 fragments, 15 tasks, a distributed sort,
dynamic filters, absolute file paths).

**Is the project complex?**
Moderately. The API, state machines, and worker registry are standard service code and are
done. The hard parts are the exchange (shuffle) operators, bridging to DataFusion's changing
APIs, failure handling when a worker dies, and debugging across several processes.

**Does the project apply core database-systems ideas?**
Yes, heavily. It uses query scheduling, distributed execution of a physical plan, stage
boundaries at exchange operators, hash-partitioned joins and aggregates, and the
Parquet/Arrow data formats. It touches little of query optimization (join ordering, cost
models) because the scheduler receives an already-optimized plan.

**What phase or task is being worked on?**
The project is in Phase 2b-2 (DataFusion integration), built in slices: the plan cutter,
shuffle operators with a local run, plan serialization, then network reads. See the progress
log in `DESIGN.md` for the current slice.

**What is the next step after the spike?**
Cut a real plan into fragments (slice 2a), make the cuts real with shuffle operators (slice
2b), build a real worker (slice 3), generate TPC-H data (slice 4), then run the scale-out
study on 1, 2, 4, and 8 workers (Phase 4).

## What each slice does

**What does slice 2a include, and what is slice 2b?**
Slice 2a is the plan cutter: `cut(plan)` analyzes a real DataFusion plan and reports the
fragments, their operators, inputs, output exchange, and task counts, without changing the
plan. Slice 2b makes the cuts real: shuffle write and read operators, a rewriter, a local
run (2b-i), plan serialization (2b-ii-a), and network reads (2b-ii-b).

**What did slices 2a, 2b-i, and 2b-ii do, and what is in the zip files?**
2a: the cutter (6 fragments, 15 tasks for the example query). 2b-i: an in-memory shuffle
store, the write and read operators, `distribute()` to rewrite a plan into fragment plans,
and `run_locally()` to run every task in one process, checked against single-node DataFusion.
2b-ii-a: a codec so fragments with the shuffle operators serialize to bytes and back. The
zips were full snapshots of the repository (without `.git`, `target`, or `Cargo.lock`),
created because patch files did not download reliably; each got a new version name so the
browser would not rename it. Only the newest matters.

**What is the logic of the rewriter?**
It walks the plan tree and, at each exchange, cuts the tree in two. The subtree below becomes
a fragment wrapped in a `ShuffleWriteExec`; the exchange is replaced in the parent by a
`ShuffleReadExec`. Three cases: a hash or round-robin `RepartitionExec` is replaced by a
reader; a merge or coalesce operator stays and each of its inputs becomes a fragment; any
other operator is rebuilt over its rebuilt children. Fragment ids come from the finished
fragment count, so deeper fragments get smaller ids and the root comes last. Readers copy the
properties of the node they replace so operators above a cut behave as before.

**Can I see an example of that logic?**
Yes: the step-by-step walkthrough on the example query. The walk descends to the deepest
exchange first, saves F0 (customers scan), then F1 (orders scan) and F2 (filter), F3 (join
plus partial aggregate), F4 (final aggregate, projection, sort), and finally the root F5
(merge).

**Explain the example like I'm five.**
Two piles of cards (names and orders) must be matched by two helpers at two tables. Everyone
uses one rule: even numbers go to Table 0, odd numbers to Table 1. Because both piles use the
same rule, a name card and its order cards always meet at the same table. If the piles used
different rules, matching cards would end up at different tables and nobody would notice the
missing matches.

**Explain the example in the real project.**
The same idea with real names: 6 fragments and 15 tasks. Customers and orders are both
hashed on the join key into 4 buckets, so join task `j` reads bucket `j` from every producer
and sees every key together with its matches. The partial aggregates are hashed by segment,
each final-aggregate task totals and sorts its segments, and the root merges the four sorted
streams into the final three rows. Counting empty buckets, the query stores 44 buckets.

## Performance and distribution

**Is the query slow because it runs in sequential order?**
Only `run_locally()` is sequential, and it is a correctness harness, not the real execution
path. Nothing has been benchmarked, and on 20,000 rows plain single-node DataFusion would
likely win. Some ordering is required (a stage cannot start before its input stage finishes);
ordering between the tasks inside a stage is not, and real workers will run those in
parallel.

**After the next slice, are tasks distributed to workers to increase speed?**
Not yet. The next slice (network reads) only makes the shuffle capable of crossing machines.
Tasks really run on different workers in slice 3 (real worker and scheduler integration), and
a measured speedup comes from the benchmark phase on large data.

**Why do we need slice 2b-ii-b, and why fetch buckets from another process instead of the
in-process map?**
The in-process map is invisible across machines. When a task finishes, its buckets sit in
the memory of the worker that ran it, and a consumer on another worker needs a way to ask for
them. Fetching directly from the producer sends data across the network once and has no
central bottleneck, unlike uploading everything to a central store. It is a separate slice so
that reading and fetching can be tested in one process, with simulated workers, before the
real worker exists.

## Git and GitHub setup

**How do I authorize GitHub over SSH?**
Generate an ed25519 key, add it to the SSH agent and macOS Keychain, add the public key under
GitHub Settings, SSH and GPG keys, and test with `ssh -T git@github.com`.

**Why did my commits appear under the wrong account?**
GitHub attributes a commit by its author email, not by the account used to push. The push was
authenticated correctly as the right account, but `git config user.email` held the other
account's email. Fix: set `user.name` and `user.email` (repo and global), then rewrite the old
commits with the new author and force-push with `--force-with-lease`.

**Should I create the repository myself?**
Yes. The assistant has no access to GitHub, so the repository is created on github.com (empty,
no README), and the assistant supplies commands and files.

**What does the real worker do, and why is the scheduler not changed in the same step?**
The worker receives a serialized fragment, runs one partition with DataFusion, stores its output buckets, serves them to other workers, and reports back to the scheduler. It is built and tested first with a stand-in scheduler so that a problem can be traced to the worker or to the scheduler, not to both at once. The scheduler integration is the next step.
