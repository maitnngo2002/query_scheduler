//! A real worker: receives a serialized plan fragment from the scheduler, runs one
//! partition of it with DataFusion, stores the output buckets, serves them to other
//! workers, and reports the result back to the scheduler.
//!
//! Flow of one task:
//!   1. `ExecuteTask` arrives with the fragment bytes, the task id, and the locations
//!      of every producer task this fragment reads from. The worker accepts at once.
//!   2. In the background it decodes the plan with a codec whose readers fetch through
//!      a router built from those locations (local buckets from memory, others over gRPC).
//!   3. It runs partition `task.partition`. A shuffle writer stores its own buckets;
//!      the root fragment has no writer, so its output is kept as bucket 0.
//!   4. It reports success or failure, with timing and output size, to the scheduler.
//!
//! Other workers (and the scheduler, for final results) read buckets through
//! `FetchTaskOutput`, served by the same bucket server used in the network tests.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::prelude::SessionContext;
use df_adapter::codec::{decode_plan, ShuffleCodec};
use df_adapter::net::{BucketServer, RoutedSource};
use df_adapter::shuffle::ShuffleWriteExec;
use df_adapter::store::{OutputKey, ShuffleStore};
use futures::TryStreamExt;
use scheduler_proto::v1::worker_registry_client::WorkerRegistryClient;
use scheduler_proto::v1::worker_service_server::{WorkerService, WorkerServiceServer};
use scheduler_proto::v1::{
    ExecuteTaskRequest, ExecuteTaskResponse, FetchTaskOutputRequest, HeartbeatRequest,
    RegisterWorkerRequest, ReportTaskStatusRequest, TaskId, TaskMetrics, TaskState,
    WorkerResources,
};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct Inner {
    scheduler_url: String,
    /// This worker's own address, as other workers and the scheduler reach it.
    own_address: String,
    store: Arc<ShuffleStore>,
    session: SessionContext,
}

impl Inner {
    /// Decodes and runs one task. Returns (output rows, output bytes).
    async fn run_task(&self, req: &ExecuteTaskRequest) -> DfResult<(u64, u64)> {
        let task = req
            .task
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("ExecuteTask has no task id".into()))?;
        let fragment = task.fragment_id as usize;
        let partition = task.partition as usize;

        let locations: HashMap<(usize, usize), String> = req
            .inputs
            .iter()
            .map(|i| {
                ((i.fragment_id as usize, i.producer_partition as usize), i.worker_address.clone())
            })
            .collect();
        let source = Arc::new(RoutedSource::new(
            Some(self.own_address.clone()),
            Arc::clone(&self.store),
            locations,
        ));
        let codec = ShuffleCodec::with_source(Arc::clone(&self.store), source)
            .with_query_id(task.query_id.clone());

        let task_ctx = self.session.task_ctx();
        let plan = decode_plan(&req.fragment_plan, &task_ctx, &codec)?;
        let batches: Vec<RecordBatch> = plan.execute(partition, task_ctx)?.try_collect().await?;

        // A shuffle writer stored its buckets while it ran. The root fragment has no
        // writer, so keep its result as bucket 0 for FetchResults.
        if !plan.is::<ShuffleWriteExec>() {
            self.store.put(
                OutputKey { query_id: task.query_id.clone(), fragment, task: partition, bucket: 0 },
                batches,
            );
        }

        let (mut rows, mut bytes) = (0u64, 0u64);
        for bucket in 0..(req.output_partitions.max(1) as usize) {
            let key = OutputKey { query_id: task.query_id.clone(), fragment, task: partition, bucket };
            if let Some(stored) = self.store.get(&key) {
                for batch in &stored {
                    rows += batch.num_rows() as u64;
                    bytes += batch.get_array_memory_size() as u64;
                }
            }
        }
        Ok((rows, bytes))
    }

    async fn report(&self, task: TaskId, outcome: DfResult<(u64, u64)>, started: u64, finished: u64) {
        let (state, error_message, rows, bytes) = match outcome {
            Ok((rows, bytes)) => (TaskState::Succeeded, String::new(), rows, bytes),
            Err(e) => (TaskState::Failed, e.to_string(), 0, 0),
        };
        let request = ReportTaskStatusRequest {
            task: Some(task),
            state: state as i32,
            error_message,
            metrics: Some(TaskMetrics {
                start_unix_ms: started,
                end_unix_ms: finished,
                output_rows: rows,
                output_bytes: bytes,
            }),
        };
        match WorkerRegistryClient::connect(self.scheduler_url.clone()).await {
            Ok(mut client) => {
                if let Err(e) = client.report_task_status(request).await {
                    eprintln!("could not report task status: {e}");
                }
            }
            Err(e) => eprintln!("could not reach the scheduler to report task status: {e}"),
        }
    }
}

/// The worker. Implements both halves of the worker API: running tasks and serving buckets.
pub struct Worker {
    inner: Arc<Inner>,
    buckets: BucketServer,
}

impl Worker {
    pub fn new(scheduler_url: String, own_address: String) -> Self {
        let store = Arc::new(ShuffleStore::new());
        Worker {
            inner: Arc::new(Inner {
                scheduler_url,
                own_address,
                store: Arc::clone(&store),
                session: SessionContext::new(),
            }),
            buckets: BucketServer::new(store),
        }
    }

    pub fn store(&self) -> Arc<ShuffleStore> {
        Arc::clone(&self.inner.store)
    }
}

#[tonic::async_trait]
impl WorkerService for Worker {
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();
        let task = req
            .task
            .clone()
            .ok_or_else(|| Status::invalid_argument("task is required"))?;
        if req.fragment_plan.is_empty() {
            return Err(Status::invalid_argument("fragment_plan must not be empty"));
        }

        // Accept immediately; the scheduler learns the outcome from ReportTaskStatus.
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let started = now_ms();
            let outcome = inner.run_task(&req).await;
            inner.report(task, outcome, started, now_ms()).await;
        });
        Ok(Response::new(ExecuteTaskResponse { accepted: true }))
    }

    type FetchTaskOutputStream = <BucketServer as WorkerService>::FetchTaskOutputStream;

    async fn fetch_task_output(
        &self,
        request: Request<FetchTaskOutputRequest>,
    ) -> Result<Response<Self::FetchTaskOutputStream>, Status> {
        self.buckets.fetch_task_output(request).await
    }
}

/// Serves the worker API on `listen` until the future is dropped.
pub async fn serve(listen: SocketAddr, worker: Worker) -> Result<(), tonic::transport::Error> {
    Server::builder().add_service(WorkerServiceServer::new(worker)).serve(listen).await
}

/// Registers with the scheduler and keeps heartbeating. If the scheduler forgets this
/// worker (for example after a restart), it registers again. Runs until dropped.
pub async fn register_and_heartbeat(scheduler_url: String, own_address: String, slots: u32) {
    loop {
        let mut client = loop {
            match WorkerRegistryClient::connect(scheduler_url.clone()).await {
                Ok(client) => break client,
                Err(e) => {
                    eprintln!("scheduler not reachable yet ({e}); retrying in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        let registration = client
            .register_worker(RegisterWorkerRequest {
                address: own_address.clone(),
                resources: Some(WorkerResources { slots, memory_bytes: 0 }),
            })
            .await;
        let response = match registration {
            Ok(r) => r.into_inner(),
            Err(e) => {
                eprintln!("registration failed ({e}); retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        eprintln!("registered as {} at {own_address}", response.worker_id);

        let interval = Duration::from_millis(response.heartbeat_interval_ms.max(100) as u64);
        loop {
            tokio::time::sleep(interval).await;
            let heartbeat = HeartbeatRequest { worker_id: response.worker_id.clone(), free_slots: slots };
            match client.heartbeat(heartbeat).await {
                Ok(r) if r.get_ref().known => {}
                Ok(_) => {
                    eprintln!("scheduler no longer knows this worker; registering again");
                    break;
                }
                Err(e) => eprintln!("heartbeat failed: {e}"),
            }
        }
    }
}
