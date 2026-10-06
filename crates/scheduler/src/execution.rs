//! Per-query execution state: which tasks are pending, running, or done, and
//! where each finished task's output lives.
//!
//! Pure logic with no I/O, so it can be unit tested without a network.

use std::collections::{HashMap, HashSet};

use fragmenter::tasks::{ready_tasks, TaskGraph, TaskId};
use fragmenter::FragmentId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskPhase {
    Pending,
    Dispatched { worker_id: String, address: String },
    Done { worker_id: String, address: String },
    Failed(String),
    Cancelled,
}

/// Where to pull one producer task's output from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputLocation {
    pub fragment: FragmentId,
    pub producer_partition: u32,
    pub address: String,
    pub bucket: u32,
}

/// Everything a worker needs to run one task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDispatch {
    pub fragment_plan: Vec<u8>,
    pub output_partitions: u32,
    pub inputs: Vec<InputLocation>,
}

pub struct QueryExecution {
    pub query_id: String,
    graph: TaskGraph,
    fragment_plans: Vec<Vec<u8>>,
    phases: HashMap<TaskId, TaskPhase>,
    completed: HashSet<TaskId>,
    failure: Option<String>,
    cancelled: bool,
}

impl QueryExecution {
    /// `fragment_plans[i]` is the serialized plan of fragment `i`.
    pub fn new(query_id: String, graph: TaskGraph, fragment_plans: Vec<Vec<u8>>) -> Self {
        let phases = graph.tasks.iter().map(|t| (t.id, TaskPhase::Pending)).collect();
        QueryExecution {
            query_id,
            graph,
            fragment_plans,
            phases,
            completed: HashSet::new(),
            failure: None,
            cancelled: false,
        }
    }

    /// Pending tasks whose input fragments have fully completed.
    /// Empty once the query has failed or been cancelled.
    pub fn dispatchable(&self) -> Vec<TaskId> {
        if self.failure.is_some() || self.cancelled {
            return Vec::new();
        }
        ready_tasks(&self.graph, &self.completed)
            .into_iter()
            .filter(|t| matches!(self.phases.get(t), Some(TaskPhase::Pending)))
            .collect()
    }

    /// Builds the dispatch payload for `task`. None if the task is unknown or
    /// any producer task has not finished.
    pub fn task_dispatch(&self, task: TaskId) -> Option<TaskDispatch> {
        let t = self.graph.tasks.iter().find(|t| t.id == task)?;
        let fragment_plan = self.fragment_plans.get(task.fragment as usize)?.clone();
        let mut inputs = Vec::new();
        for spec in &t.inputs {
            for p in 0..spec.producer_tasks {
                let producer = TaskId { fragment: spec.fragment, partition: p };
                let address = match self.phases.get(&producer) {
                    Some(TaskPhase::Done { address, .. }) => address.clone(),
                    _ => return None,
                };
                inputs.push(InputLocation {
                    fragment: spec.fragment,
                    producer_partition: p,
                    address,
                    bucket: spec.bucket,
                });
            }
        }
        Some(TaskDispatch { fragment_plan, output_partitions: t.output_partitions, inputs })
    }

    /// Pending -> Dispatched. False if the task is not pending.
    pub fn mark_dispatched(&mut self, task: TaskId, worker_id: String, address: String) -> bool {
        if let Some(p) = self.phases.get_mut(&task) {
            if *p == TaskPhase::Pending {
                *p = TaskPhase::Dispatched { worker_id, address };
                return true;
            }
        }
        false
    }

    /// Dispatched -> Pending, for a dispatch that did not reach the worker.
    pub fn revert_dispatch(&mut self, task: TaskId) {
        if let Some(p) = self.phases.get_mut(&task) {
            if matches!(&*p, TaskPhase::Dispatched { .. }) {
                *p = TaskPhase::Pending;
            }
        }
    }

    /// Dispatched -> Done. Returns the worker whose slot to release, or None if
    /// the task was not dispatched (duplicate or late report).
    pub fn on_success(&mut self, task: TaskId) -> Option<String> {
        let p = self.phases.get_mut(&task)?;
        let (worker_id, address) = match &*p {
            TaskPhase::Dispatched { worker_id, address } => (worker_id.clone(), address.clone()),
            _ => return None,
        };
        *p = TaskPhase::Done { worker_id: worker_id.clone(), address };
        self.completed.insert(task);
        Some(worker_id)
    }

    /// Dispatched -> Failed, and the whole query is marked failed. Returns the
    /// worker whose slot to release, or None for a duplicate or late report.
    pub fn on_failure(&mut self, task: TaskId, message: String) -> Option<String> {
        let p = self.phases.get_mut(&task)?;
        let worker_id = match &*p {
            TaskPhase::Dispatched { worker_id, .. } => worker_id.clone(),
            _ => return None,
        };
        *p = TaskPhase::Failed(message.clone());
        if self.failure.is_none() {
            self.failure = Some(message);
        }
        Some(worker_id)
    }

    /// Cancels every unfinished task. Returns the workers whose slots to release.
    pub fn cancel(&mut self) -> Vec<String> {
        self.cancelled = true;
        let mut workers = Vec::new();
        for p in self.phases.values_mut() {
            let in_flight = match &*p {
                TaskPhase::Dispatched { worker_id, .. } => Some(worker_id.clone()),
                _ => None,
            };
            if let Some(w) = in_flight {
                workers.push(w);
                *p = TaskPhase::Cancelled;
            } else if *p == TaskPhase::Pending {
                *p = TaskPhase::Cancelled;
            }
        }
        workers
    }

    /// Puts tasks that were running on a lost worker back to Pending.
    /// Only in-flight tasks are recovered; outputs of finished tasks on that
    /// worker are lost (a known gap, see the design doc).
    pub fn requeue_worker(&mut self, worker_id: &str) -> usize {
        let mut n = 0;
        for p in self.phases.values_mut() {
            let hit = matches!(&*p, TaskPhase::Dispatched { worker_id: w, .. } if w.as_str() == worker_id);
            if hit {
                *p = TaskPhase::Pending;
                n += 1;
            }
        }
        n
    }

    /// The finished root-fragment tasks and the workers holding their output,
    /// ordered by partition. None until every root task is done.
    /// The root fragment is always the last one (ids are assigned in post-order).
    pub fn root_outputs(&self) -> Option<Vec<(TaskId, String)>> {
        let root = self.graph.fragment_task_counts.len().checked_sub(1)? as FragmentId;
        let mut out = Vec::new();
        for p in 0..self.graph.fragment_task_counts[root as usize] {
            let id = TaskId { fragment: root, partition: p };
            match self.phases.get(&id) {
                Some(TaskPhase::Done { address, .. }) => out.push((id, address.clone())),
                _ => return None,
            }
        }
        Some(out)
    }

    pub fn is_complete(&self) -> bool {
        self.failure.is_none() && !self.cancelled && self.completed.len() == self.graph.tasks.len()
    }

    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// All tasks and their phases, sorted by task id.
    pub fn task_phases(&self) -> Vec<(TaskId, TaskPhase)> {
        let mut v: Vec<(TaskId, TaskPhase)> =
            self.phases.iter().map(|(k, v)| (*k, v.clone())).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fragmenter::tasks::{expand, ExpandConfig};
    use fragmenter::{fragment, ExchangeKind, PlanNode};

    fn t(fragment: u32, partition: u32) -> TaskId {
        TaskId { fragment, partition }
    }

    /// Join(Exchange(Hash, Scan a), Exchange(Hash, Scan b)) with 2 shuffle partitions:
    /// tasks (0,0), (1,0), (2,0), (2,1).
    fn join_exec() -> QueryExecution {
        let side = |table: &str| PlanNode::Exchange {
            kind: ExchangeKind::Hash(vec!["k".to_string()]),
            input: Box::new(PlanNode::Scan { table: table.to_string() }),
        };
        let plan = PlanNode::Join {
            on: "a.k = b.k".to_string(),
            left: Box::new(side("a")),
            right: Box::new(side("b")),
        };
        let fp = fragment(plan).unwrap();
        let cfg = ExpandConfig {
            shuffle_partitions: 2,
            default_scan_splits: 1,
            scan_splits: HashMap::new(),
        };
        let graph = expand(&fp, &cfg);
        let plans = vec![Vec::new(); fp.fragments.len()];
        QueryExecution::new("q-0".to_string(), graph, plans)
    }

    fn dispatch(e: &mut QueryExecution, task: TaskId, worker: &str) {
        assert!(e.mark_dispatched(task, worker.to_string(), format!("http://{worker}")));
    }

    #[test]
    fn only_scans_are_dispatchable_at_first() {
        let e = join_exec();
        assert_eq!(e.dispatchable(), vec![t(0, 0), t(1, 0)]);
    }

    #[test]
    fn full_flow_respects_stage_barrier_and_input_locations() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        dispatch(&mut e, t(1, 0), "wb");
        assert!(e.dispatchable().is_empty(), "dispatched tasks are not dispatchable again");

        assert_eq!(e.on_success(t(0, 0)), Some("wa".to_string()));
        assert!(e.dispatchable().is_empty(), "join still needs the second scan");
        assert!(e.task_dispatch(t(2, 1)).is_none(), "producers must be done");

        assert_eq!(e.on_success(t(1, 0)), Some("wb".to_string()));
        assert_eq!(e.dispatchable(), vec![t(2, 0), t(2, 1)]);

        let d = e.task_dispatch(t(2, 1)).unwrap();
        assert_eq!(
            d.inputs,
            vec![
                InputLocation { fragment: 0, producer_partition: 0, address: "http://wa".to_string(), bucket: 1 },
                InputLocation { fragment: 1, producer_partition: 0, address: "http://wb".to_string(), bucket: 1 },
            ]
        );
        assert_eq!(d.output_partitions, 1, "root fragment output is not repartitioned");

        dispatch(&mut e, t(2, 0), "wa");
        dispatch(&mut e, t(2, 1), "wb");
        assert!(!e.is_complete());
        assert!(e.root_outputs().is_none());
        e.on_success(t(2, 0));
        assert!(e.root_outputs().is_none(), "one root task is still running");
        e.on_success(t(2, 1));
        assert!(e.is_complete());
        assert_eq!(
            e.root_outputs(),
            Some(vec![
                (t(2, 0), "http://wa".to_string()),
                (t(2, 1), "http://wb".to_string()),
            ])
        );
    }

    #[test]
    fn mark_dispatched_twice_is_rejected() {
        let mut e = join_exec();
        assert!(e.mark_dispatched(t(0, 0), "w".to_string(), "http://w".to_string()));
        assert!(!e.mark_dispatched(t(0, 0), "w2".to_string(), "http://w2".to_string()));
    }

    #[test]
    fn duplicate_success_reports_are_ignored() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        assert_eq!(e.on_success(t(0, 0)), Some("wa".to_string()));
        assert_eq!(e.on_success(t(0, 0)), None);
    }

    #[test]
    fn failure_stops_dispatch_and_fails_the_query() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        assert_eq!(e.on_failure(t(0, 0), "boom".to_string()), Some("wa".to_string()));
        assert_eq!(e.failure(), Some("boom"));
        assert!(e.dispatchable().is_empty());
        assert!(!e.is_complete());
    }

    #[test]
    fn cancel_returns_in_flight_workers_and_ignores_late_reports() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        assert_eq!(e.cancel(), vec!["wa".to_string()]);
        assert!(e.is_cancelled());
        assert!(e.dispatchable().is_empty());
        assert_eq!(e.on_success(t(0, 0)), None);
        assert!(e.task_phases().iter().all(|(_, p)| *p == TaskPhase::Cancelled));
    }

    #[test]
    fn requeue_puts_in_flight_tasks_of_a_lost_worker_back() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        dispatch(&mut e, t(1, 0), "wb");
        assert_eq!(e.requeue_worker("wa"), 1);
        assert_eq!(e.dispatchable(), vec![t(0, 0)]);
    }

    #[test]
    fn revert_dispatch_makes_a_task_pending_again() {
        let mut e = join_exec();
        dispatch(&mut e, t(0, 0), "wa");
        e.revert_dispatch(t(0, 0));
        assert_eq!(e.dispatchable(), vec![t(0, 0), t(1, 0)]);
    }
}
