//! Scheduler engine: turns submitted plans into task executions, dispatches
//! ready tasks to workers, and applies task status reports.
//!
//! Dispatch runs in one background loop (`run_dispatcher`). It wakes when a
//! query is submitted or a task finishes, and also on a short timer so that
//! tasks waiting for capacity are retried.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fragmenter::tasks::{expand, ExpandConfig, TaskId};
use fragmenter::{fragment, PlanNode};
use scheduler_proto::v1::worker_service_client::WorkerServiceClient;
use scheduler_proto::v1::{
    ExecuteTaskRequest, ProducerLocation, QueryState, TaskId as ProtoTaskId, TaskState,
};
use tokio::sync::Notify;

use crate::execution::{QueryExecution, TaskPhase};
use crate::queries::{QueryError, QueryManager};
use crate::workers::WorkerManager;

#[derive(Debug, PartialEq, Eq)]
pub enum SubmitError {
    BadPlan(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResultsError {
    NotFound,
    NotReady(String),
}

pub struct Engine {
    pub queries: QueryManager,
    pub workers: WorkerManager,
    executions: Mutex<HashMap<String, QueryExecution>>,
    expand_cfg: ExpandConfig,
    wake: Notify,
}

impl Engine {
    pub fn new(expand_cfg: ExpandConfig) -> Self {
        Engine {
            queries: QueryManager::new(),
            workers: WorkerManager::new(),
            executions: Mutex::new(HashMap::new()),
            expand_cfg,
            wake: Notify::new(),
        }
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Decodes the plan, fragments it, expands it into tasks, and registers the
    /// query. Dispatch happens asynchronously.
    ///
    /// Interim plan format: the JSON encoding of `fragmenter::PlanNode`. A
    /// DataFusion plan adapter replaces this in the next phase.
    pub fn submit(&self, plan: &[u8], priority: i32) -> Result<String, SubmitError> {
        let node: PlanNode = serde_json::from_slice(plan)
            .map_err(|e| SubmitError::BadPlan(format!("plan is not valid JSON: {e}")))?;
        let fp = fragment(node).map_err(|e| SubmitError::BadPlan(e.to_string()))?;
        let graph = expand(&fp, &self.expand_cfg);
        let fragment_plans = fp
            .fragments
            .iter()
            .map(|f| serde_json::to_vec(&f.root))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SubmitError::BadPlan(e.to_string()))?;

        let query_id = self.queries.submit(priority);
        self.executions
            .lock()
            .unwrap()
            .insert(query_id.clone(), QueryExecution::new(query_id.clone(), graph, fragment_plans));
        self.wake();
        Ok(query_id)
    }

    /// Applies a worker's status report for one task.
    pub fn on_task_report(&self, query_id: &str, task: TaskId, state: TaskState, error: &str) {
        let mut execs = self.executions.lock().unwrap();
        let Some(exec) = execs.get_mut(query_id) else {
            return;
        };
        let released = match state {
            TaskState::Succeeded => exec.on_success(task),
            TaskState::Failed => exec.on_failure(task, error.to_string()),
            _ => None,
        };
        if let Some(worker_id) = released {
            self.workers.release(&worker_id);
        }
        if exec.failure().is_some() {
            let _ = self.queries.transition(query_id, QueryState::Running);
            let _ = self.queries.transition(query_id, QueryState::Failed);
        } else if exec.is_complete() {
            let _ = self.queries.transition(query_id, QueryState::Running);
            let _ = self.queries.transition(query_id, QueryState::Succeeded);
        }
        drop(execs);
        self.wake();
    }

    /// Cancels a query and frees the slots of its in-flight tasks.
    /// (Workers are not told to stop yet; their late reports are ignored.)
    pub fn cancel(&self, query_id: &str) -> Result<(), QueryError> {
        self.queries.cancel(query_id)?;
        let mut execs = self.executions.lock().unwrap();
        if let Some(exec) = execs.get_mut(query_id) {
            for worker_id in exec.cancel() {
                self.workers.release(&worker_id);
            }
        }
        drop(execs);
        self.wake();
        Ok(())
    }

    /// Re-queues in-flight tasks of workers that were evicted.
    pub fn handle_evicted_workers(&self, worker_ids: &[String]) {
        if worker_ids.is_empty() {
            return;
        }
        let mut execs = self.executions.lock().unwrap();
        for exec in execs.values_mut() {
            for id in worker_ids {
                let n = exec.requeue_worker(id);
                if n > 0 {
                    eprintln!("re-queued {n} task(s) of query {} from lost worker {id}", exec.query_id);
                }
            }
        }
        drop(execs);
        self.wake();
    }

    /// Where to fetch a finished query's results: one (task, worker address)
    /// per root-fragment task, ordered by partition.
    pub fn result_sources(&self, query_id: &str) -> Result<Vec<(TaskId, String)>, ResultsError> {
        match self.queries.status(query_id) {
            Err(_) => return Err(ResultsError::NotFound),
            Ok(QueryState::Succeeded) => {}
            Ok(other) => return Err(ResultsError::NotReady(format!("query is {:?}", other))),
        }
        let execs = self.executions.lock().unwrap();
        let exec = execs.get(query_id).ok_or(ResultsError::NotFound)?;
        let sources = exec.root_outputs();
        sources.ok_or_else(|| ResultsError::NotReady("root task outputs are not available".to_string()))
    }

    pub fn task_phases(&self, query_id: &str) -> Vec<(TaskId, TaskPhase)> {
        self.executions
            .lock()
            .unwrap()
            .get(query_id)
            .map(|e| e.task_phases())
            .unwrap_or_default()
    }

    pub fn failure(&self, query_id: &str) -> Option<String> {
        self.executions
            .lock()
            .unwrap()
            .get(query_id)
            .and_then(|e| e.failure().map(|s| s.to_string()))
    }

    /// Background loop that dispatches ready tasks. Runs until the process exits.
    pub async fn run_dispatcher(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            self.dispatch_round().await;
        }
    }

    /// One pass: assign every dispatchable task to a worker with a free slot.
    async fn dispatch_round(&self) {
        let work: Vec<(String, TaskId)> = {
            let execs = self.executions.lock().unwrap();
            let items: Vec<(String, TaskId)> = execs
                .values()
                .flat_map(|e| {
                    let q = e.query_id.clone();
                    e.dispatchable().into_iter().map(move |t| (q.clone(), t))
                })
                .collect();
            items
        };

        for (query_id, task) in work {
            let Some(worker) = self.workers.acquire() else {
                break; // no capacity right now; the timer will retry
            };

            // Reserve the task and build its request while holding the lock,
            // then release the lock before any network call.
            let prepared = {
                let mut execs = self.executions.lock().unwrap();
                let info = execs.get_mut(&query_id).and_then(|e| {
                    let info = e.task_dispatch(task)?;
                    if !e.mark_dispatched(task, worker.id.clone(), worker.address.clone()) {
                        return None;
                    }
                    Some(info)
                });
                info
            };
            let Some(info) = prepared else {
                self.workers.release(&worker.id);
                continue;
            };

            let _ = self.queries.transition(&query_id, QueryState::Running);

            let request = ExecuteTaskRequest {
                task: Some(ProtoTaskId {
                    query_id: query_id.clone(),
                    fragment_id: task.fragment,
                    partition: task.partition,
                }),
                fragment_plan: info.fragment_plan,
                output_partitions: info.output_partitions,
                inputs: info
                    .inputs
                    .into_iter()
                    .map(|i| ProducerLocation {
                        fragment_id: i.fragment,
                        producer_partition: i.producer_partition,
                        worker_address: i.address,
                        bucket: i.bucket,
                    })
                    .collect(),
            };

            let accepted = match send_task(&worker.address, request).await {
                Ok(true) => true,
                Ok(false) => {
                    eprintln!("worker {} rejected task {:?}", worker.id, task);
                    false
                }
                Err(e) => {
                    eprintln!("could not dispatch task {:?} to {}: {e}", task, worker.id);
                    false
                }
            };
            if !accepted {
                if let Some(e) = self.executions.lock().unwrap().get_mut(&query_id) {
                    e.revert_dispatch(task);
                }
                self.workers.release(&worker.id);
            }
        }
    }
}

// A new connection per task is simple but wasteful; reuse channels later.
async fn send_task(
    address: &str,
    request: ExecuteTaskRequest,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = WorkerServiceClient::connect(address.to_string()).await?;
    let response = client.execute_task(request).await?;
    Ok(response.into_inner().accepted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> Engine {
        Engine::new(ExpandConfig::default())
    }

    fn scan_plan() -> Vec<u8> {
        serde_json::to_vec(&PlanNode::Scan { table: "t".to_string() }).unwrap()
    }

    /// Simulates what the dispatcher does for the only task of a scan query.
    fn dispatch_only_task(e: &Engine, query_id: &str) -> TaskId {
        let task = TaskId { fragment: 0, partition: 0 };
        let mut execs = e.executions.lock().unwrap();
        let exec = execs.get_mut(query_id).unwrap();
        assert!(exec.mark_dispatched(task, "w-0".to_string(), "http://w0".to_string()));
        task
    }

    #[test]
    fn rejects_invalid_json() {
        let e = engine();
        assert!(matches!(e.submit(b"not json", 0), Err(SubmitError::BadPlan(_))));
    }

    #[test]
    fn rejects_already_fragmented_plans() {
        let e = engine();
        let plan = serde_json::to_vec(&PlanNode::RemoteRead { fragment_id: 0 }).unwrap();
        assert!(matches!(e.submit(&plan, 0), Err(SubmitError::BadPlan(_))));
    }

    #[test]
    fn example_plan_file_parses_and_expands() {
        // Default config: 4 shuffle partitions, 1 split per scan.
        // F0 1 + F1 1 + F2 4 + F3 4 + F4 1 = 11 tasks.
        let e = engine();
        let plan = include_str!("../../../examples/join_agg.json");
        let id = e.submit(plan.as_bytes(), 0).unwrap();
        assert_eq!(e.task_phases(&id).len(), 11);
        assert_eq!(e.queries.status(&id), Ok(QueryState::Queued));
    }

    #[test]
    fn successful_report_completes_the_query() {
        let e = engine();
        let id = e.submit(&scan_plan(), 0).unwrap();
        let task = dispatch_only_task(&e, &id);
        e.on_task_report(&id, task, TaskState::Succeeded, "");
        assert_eq!(e.queries.status(&id), Ok(QueryState::Succeeded));
    }

    #[test]
    fn failed_report_fails_the_query_with_the_message() {
        let e = engine();
        let id = e.submit(&scan_plan(), 0).unwrap();
        let task = dispatch_only_task(&e, &id);
        e.on_task_report(&id, task, TaskState::Failed, "disk on fire");
        assert_eq!(e.queries.status(&id), Ok(QueryState::Failed));
        assert_eq!(e.failure(&id), Some("disk on fire".to_string()));
    }

    #[test]
    fn report_releases_the_workers_slot() {
        let e = engine();
        let w = e.workers.register("http://w0".to_string(), 1);
        let id = e.submit(&scan_plan(), 0).unwrap();

        assert_eq!(e.workers.acquire().unwrap().id, w);
        assert!(e.workers.acquire().is_none());

        let task = TaskId { fragment: 0, partition: 0 };
        {
            let mut execs = e.executions.lock().unwrap();
            execs.get_mut(&id).unwrap().mark_dispatched(task, w.clone(), "http://w0".to_string());
        }
        e.on_task_report(&id, task, TaskState::Succeeded, "");
        assert!(e.workers.acquire().is_some(), "slot was released");
    }

    #[test]
    fn result_sources_require_a_finished_query() {
        let e = engine();
        assert_eq!(e.result_sources("q-404"), Err(ResultsError::NotFound));

        let id = e.submit(&scan_plan(), 0).unwrap();
        assert!(matches!(e.result_sources(&id), Err(ResultsError::NotReady(_))));

        let task = dispatch_only_task(&e, &id);
        e.on_task_report(&id, task, TaskState::Succeeded, "");
        assert_eq!(
            e.result_sources(&id),
            Ok(vec![(TaskId { fragment: 0, partition: 0 }, "http://w0".to_string())])
        );
    }

    #[test]
    fn cancel_marks_the_query_cancelled_and_stops_dispatch() {
        let e = engine();
        let id = e.submit(&scan_plan(), 0).unwrap();
        e.cancel(&id).unwrap();
        assert_eq!(e.queries.status(&id), Ok(QueryState::Cancelled));
        assert!(e.executions.lock().unwrap().get(&id).unwrap().dispatchable().is_empty());
    }

    #[test]
    fn cancel_of_unknown_query_is_not_found() {
        assert_eq!(engine().cancel("q-404"), Err(QueryError::NotFound));
    }

    #[test]
    fn reports_for_unknown_queries_are_ignored() {
        let e = engine();
        e.on_task_report("q-404", TaskId { fragment: 0, partition: 0 }, TaskState::Succeeded, "");
    }
}
