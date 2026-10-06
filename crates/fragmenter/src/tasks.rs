//! Task expansion.
//!
//! A fragment is a subtree of the plan; a *task* is one partition of one
//! fragment and is the unit the scheduler assigns to a worker. Expanding
//! fragments into tasks is what lets a single stage run on many workers.
//!
//! Task count per fragment:
//!   1. If the fragment reads a Hash or RoundRobin exchange: `shuffle_partitions`
//!      tasks. Task `j` reads bucket `j` from every producer task.
//!   2. Else if it scans tables: one task per scan split (the max over the
//!      fragment's scans, since each task scans the same split index).
//!   3. Else (Coalesce / Broadcast inputs only): one task.
//!
//! A fragment that mixes a Coalesce input with multiple scan splits would read
//! duplicate data; real plans do not do this, and it is not validated here.
//!
//! Stages use a strict barrier: a task is ready once every task of every input
//! fragment has completed.

use std::collections::{HashMap, HashSet};

use crate::{ExchangeKind, FragmentId, FragmentedPlan, PlanNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId {
    pub fragment: FragmentId,
    pub partition: u32,
}

/// One input of a task: read `bucket` from each of the `producer_tasks` tasks
/// (partitions `0..producer_tasks`) of `fragment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputSpec {
    pub fragment: FragmentId,
    pub producer_tasks: u32,
    pub bucket: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    /// Number of hash buckets this task writes; 1 if its output is not repartitioned.
    pub output_partitions: u32,
    pub inputs: Vec<InputSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskGraph {
    pub tasks: Vec<Task>,
    /// Number of tasks per fragment, indexed by fragment id.
    pub fragment_task_counts: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct ExpandConfig {
    /// Partitions per hash/round-robin shuffle (clamped to at least 1).
    pub shuffle_partitions: u32,
    /// Scan splits for tables not listed in `scan_splits` (clamped to at least 1).
    pub default_scan_splits: u32,
    /// Scan splits per table, for example one per Parquet row-group range.
    pub scan_splits: HashMap<String, u32>,
}

impl Default for ExpandConfig {
    fn default() -> Self {
        ExpandConfig {
            shuffle_partitions: 4,
            default_scan_splits: 1,
            scan_splits: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct NodeInfo {
    scans: Vec<String>,
    reads: Vec<FragmentId>,
}

fn collect(node: &PlanNode, info: &mut NodeInfo) {
    match node {
        PlanNode::Scan { table } => info.scans.push(table.clone()),
        PlanNode::RemoteRead { fragment_id } => info.reads.push(*fragment_id),
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. }
        | PlanNode::ShuffleWrite { input, .. } => collect(input, info),
        PlanNode::Join { left, right, .. } => {
            collect(left, info);
            collect(right, info);
        }
    }
}

/// The exchange a fragment's output goes through, or None for the root fragment.
fn output_kind(plan: &FragmentedPlan, id: FragmentId) -> Option<&ExchangeKind> {
    match &plan.fragments.get(id as usize)?.root {
        PlanNode::ShuffleWrite { kind, .. } => Some(kind),
        _ => None,
    }
}

fn is_repartition(kind: Option<&ExchangeKind>) -> bool {
    matches!(kind, Some(ExchangeKind::Hash(_)) | Some(ExchangeKind::RoundRobin))
}

/// Expand every fragment into tasks.
pub fn expand(plan: &FragmentedPlan, cfg: &ExpandConfig) -> TaskGraph {
    let p = cfg.shuffle_partitions.max(1);

    let infos: Vec<NodeInfo> = plan
        .fragments
        .iter()
        .map(|f| {
            let mut info = NodeInfo::default();
            collect(&f.root, &mut info);
            info
        })
        .collect();

    let mut counts: Vec<u32> = Vec::with_capacity(plan.fragments.len());
    for info in &infos {
        let reads_repartitioned = info.reads.iter().any(|id| is_repartition(output_kind(plan, *id)));
        let n = if reads_repartitioned {
            p
        } else if !info.scans.is_empty() {
            info.scans
                .iter()
                .map(|t| cfg.scan_splits.get(t).copied().unwrap_or(cfg.default_scan_splits).max(1))
                .max()
                .unwrap_or(1)
        } else {
            1
        };
        counts.push(n);
    }

    let mut tasks = Vec::new();
    for (f, info) in plan.fragments.iter().zip(infos.iter()) {
        let n = counts[f.id as usize];
        let output_partitions = if is_repartition(output_kind(plan, f.id)) { p } else { 1 };
        for part in 0..n {
            let inputs = info
                .reads
                .iter()
                .map(|&id| InputSpec {
                    fragment: id,
                    producer_tasks: counts[id as usize],
                    bucket: if is_repartition(output_kind(plan, id)) { part } else { 0 },
                })
                .collect();
            tasks.push(Task {
                id: TaskId { fragment: f.id, partition: part },
                output_partitions,
                inputs,
            });
        }
    }

    TaskGraph { tasks, fragment_task_counts: counts }
}

/// Tasks not yet completed whose input fragments have all fully completed.
/// This does not exclude tasks that were already dispatched; the caller tracks those.
pub fn ready_tasks(graph: &TaskGraph, completed: &HashSet<TaskId>) -> Vec<TaskId> {
    graph
        .tasks
        .iter()
        .filter(|t| !completed.contains(&t.id))
        .filter(|t| {
            t.inputs.iter().all(|i| {
                (0..i.producer_tasks)
                    .all(|p| completed.contains(&TaskId { fragment: i.fragment, partition: p }))
            })
        })
        .map(|t| t.id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{fragment, AggMode};

    fn scan(t: &str) -> PlanNode {
        PlanNode::Scan { table: t.to_string() }
    }
    fn exchange(kind: ExchangeKind, input: PlanNode) -> PlanNode {
        PlanNode::Exchange { kind, input: Box::new(input) }
    }
    fn hash(k: &str) -> ExchangeKind {
        ExchangeKind::Hash(vec![k.to_string()])
    }

    /// The join + aggregate + sort query from the design doc.
    fn example_plan() -> PlanNode {
        let left = exchange(
            hash("cust_id"),
            PlanNode::Filter { predicate: "order_date >= 2024-01-01".to_string(), input: Box::new(scan("orders")) },
        );
        let right = exchange(hash("id"), scan("customers"));
        let join = PlanNode::Join { on: "o.cust_id = c.id".to_string(), left: Box::new(left), right: Box::new(right) };
        let partial = PlanNode::Aggregate { mode: AggMode::Partial, group_by: vec!["segment".to_string()], input: Box::new(join) };
        let final_agg = PlanNode::Aggregate {
            mode: AggMode::Final,
            group_by: vec!["segment".to_string()],
            input: Box::new(exchange(hash("segment"), partial)),
        };
        PlanNode::Sort {
            keys: vec!["revenue".to_string()],
            input: Box::new(exchange(ExchangeKind::Coalesce, final_agg)),
        }
    }

    fn example_cfg() -> ExpandConfig {
        let mut scan_splits = HashMap::new();
        scan_splits.insert("orders".to_string(), 8);
        scan_splits.insert("customers".to_string(), 2);
        ExpandConfig { shuffle_partitions: 4, default_scan_splits: 1, scan_splits }
    }

    fn complete_fragment(graph: &TaskGraph, done: &mut HashSet<TaskId>, fragment: FragmentId) {
        for t in &graph.tasks {
            if t.id.fragment == fragment {
                done.insert(t.id);
            }
        }
    }

    #[test]
    fn worked_example_task_counts() {
        let fp = fragment(example_plan()).unwrap();
        assert_eq!(fp.fragments.len(), 5);
        let g = expand(&fp, &example_cfg());
        assert_eq!(g.fragment_task_counts, vec![8, 2, 4, 4, 1]);
        assert_eq!(g.tasks.len(), 19);
    }

    #[test]
    fn worked_example_output_partitions() {
        let fp = fragment(example_plan()).unwrap();
        let g = expand(&fp, &example_cfg());
        let out = |frag: FragmentId| {
            g.tasks.iter().find(|t| t.id.fragment == frag).unwrap().output_partitions
        };
        assert_eq!(out(0), 4);
        assert_eq!(out(1), 4);
        assert_eq!(out(2), 4);
        assert_eq!(out(3), 1); // Coalesce
        assert_eq!(out(4), 1); // root
    }

    #[test]
    fn hash_consumers_read_their_own_bucket_from_every_producer() {
        let fp = fragment(example_plan()).unwrap();
        let g = expand(&fp, &example_cfg());

        let t = g.tasks.iter().find(|t| t.id == TaskId { fragment: 2, partition: 2 }).unwrap();
        assert_eq!(
            t.inputs,
            vec![
                InputSpec { fragment: 0, producer_tasks: 8, bucket: 2 },
                InputSpec { fragment: 1, producer_tasks: 2, bucket: 2 },
            ]
        );

        let t = g.tasks.iter().find(|t| t.id == TaskId { fragment: 3, partition: 3 }).unwrap();
        assert_eq!(t.inputs, vec![InputSpec { fragment: 2, producer_tasks: 4, bucket: 3 }]);
    }

    #[test]
    fn coalesce_consumer_reads_bucket_zero_from_all_producers() {
        let fp = fragment(example_plan()).unwrap();
        let g = expand(&fp, &example_cfg());
        let t = g.tasks.iter().find(|t| t.id == TaskId { fragment: 4, partition: 0 }).unwrap();
        assert_eq!(t.inputs, vec![InputSpec { fragment: 3, producer_tasks: 4, bucket: 0 }]);
    }

    #[test]
    fn ready_tasks_follow_stage_barriers() {
        let fp = fragment(example_plan()).unwrap();
        let g = expand(&fp, &example_cfg());
        let mut done: HashSet<TaskId> = HashSet::new();

        // Both scans are ready: 8 + 2 tasks.
        let ready = ready_tasks(&g, &done);
        assert_eq!(ready.len(), 10);
        assert!(ready.iter().all(|t| t.fragment <= 1));

        // Finishing only F0 does not unblock F2 (it also needs F1).
        complete_fragment(&g, &mut done, 0);
        let ready = ready_tasks(&g, &done);
        assert_eq!(ready.len(), 2);
        assert!(ready.iter().all(|t| t.fragment == 1));

        // One unfinished F1 task still blocks F2.
        done.insert(TaskId { fragment: 1, partition: 0 });
        assert_eq!(ready_tasks(&g, &done), vec![TaskId { fragment: 1, partition: 1 }]);

        // Both done: the four F2 tasks are ready.
        done.insert(TaskId { fragment: 1, partition: 1 });
        let ready = ready_tasks(&g, &done);
        assert_eq!(ready.len(), 4);
        assert!(ready.iter().all(|t| t.fragment == 2));

        complete_fragment(&g, &mut done, 2);
        assert_eq!(ready_tasks(&g, &done).len(), 4); // F3

        complete_fragment(&g, &mut done, 3);
        assert_eq!(ready_tasks(&g, &done), vec![TaskId { fragment: 4, partition: 0 }]);

        complete_fragment(&g, &mut done, 4);
        assert!(ready_tasks(&g, &done).is_empty());
    }

    #[test]
    fn single_scan_defaults_to_one_task() {
        let fp = fragment(scan("t")).unwrap();
        let g = expand(&fp, &ExpandConfig::default());
        assert_eq!(g.fragment_task_counts, vec![1]);
        assert_eq!(g.tasks.len(), 1);
        assert_eq!(g.tasks[0].output_partitions, 1);
        assert!(g.tasks[0].inputs.is_empty());
    }

    #[test]
    fn zero_partition_and_split_settings_are_clamped_to_one() {
        let fp = fragment(exchange(hash("k"), scan("t"))).unwrap();
        let cfg = ExpandConfig { shuffle_partitions: 0, default_scan_splits: 0, scan_splits: HashMap::new() };
        let g = expand(&fp, &cfg);
        assert_eq!(g.fragment_task_counts, vec![1, 1]);
        assert_eq!(g.tasks[0].output_partitions, 1);
    }

    #[test]
    fn broadcast_join_keeps_probe_side_scan_splits() {
        // Join(Broadcast(small), big): the big scan is split, and every task
        // reads the full broadcast output (bucket 0).
        let plan = PlanNode::Join {
            on: "a = b".to_string(),
            left: Box::new(exchange(ExchangeKind::Broadcast, scan("small"))),
            right: Box::new(scan("big")),
        };
        let fp = fragment(plan).unwrap();
        let mut scan_splits = HashMap::new();
        scan_splits.insert("big".to_string(), 6);
        let g = expand(&fp, &ExpandConfig { shuffle_partitions: 4, default_scan_splits: 1, scan_splits });

        assert_eq!(g.fragment_task_counts, vec![1, 6]);
        let producer = g.tasks.iter().find(|t| t.id.fragment == 0).unwrap();
        assert_eq!(producer.output_partitions, 1);
        for t in g.tasks.iter().filter(|t| t.id.fragment == 1) {
            assert_eq!(t.inputs, vec![InputSpec { fragment: 0, producer_tasks: 1, bucket: 0 }]);
        }
    }
}
