//! Mock worker: registers with the scheduler, heartbeats, and "executes"
//! fragments by sleeping and then reporting success. Used to develop and test
//! the scheduler without a real execution engine. Executes one task (one
//! partition of a fragment) at a time per request.
//!
//! Env vars:
//!   SCHEDULER_ADDR   scheduler endpoint     (default http://127.0.0.1:50051)
//!   WORKER_LISTEN    bind address           (default 127.0.0.1:50061)
//!   WORKER_ADVERTISE address the scheduler dials (default http://<WORKER_LISTEN>)
//!   WORKER_SLOTS     concurrent fragments   (default 4)
//!   TASK_DELAY_MS    simulated work time    (default 100)

use std::net::SocketAddr;
use std::time::Duration;

use scheduler_proto::v1::worker_registry_client::WorkerRegistryClient;
use scheduler_proto::v1::worker_service_server::{WorkerService, WorkerServiceServer};
use scheduler_proto::v1::{
    ExecuteTaskRequest, ExecuteTaskResponse, FetchTaskOutputRequest, HeartbeatRequest,
    OutputChunk, RegisterWorkerRequest, ReportTaskStatusRequest, TaskState, WorkerResources,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

struct MockWorker {
    scheduler_addr: String,
    delay: Duration,
}

#[tonic::async_trait]
impl WorkerService for MockWorker {
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();
        let scheduler_addr = self.scheduler_addr.clone();
        let delay = self.delay;

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            match WorkerRegistryClient::connect(scheduler_addr).await {
                Ok(mut client) => {
                    let report = ReportTaskStatusRequest {
                        task: req.task,
                        state: TaskState::Succeeded as i32,
                        error_message: String::new(),
                        metrics: None,
                    };
                    if let Err(e) = client.report_task_status(report).await {
                        eprintln!("failed to report task status: {e}");
                    }
                }
                Err(e) => eprintln!("failed to connect to scheduler: {e}"),
            }
        });

        Ok(Response::new(ExecuteTaskResponse { accepted: true }))
    }

    type FetchTaskOutputStream = ReceiverStream<Result<OutputChunk, Status>>;

    /// Returns a canned payload that identifies the task and bucket, so tests
    /// and demos can tell which task a result came from.
    async fn fetch_task_output(
        &self,
        request: Request<FetchTaskOutputRequest>,
    ) -> Result<Response<Self::FetchTaskOutputStream>, Status> {
        let req = request.into_inner();
        let task = req.task.unwrap_or_default();
        let payload = format!(
            "mock-result:{}:{}:{}:{}",
            task.query_id, task.fragment_id, task.partition, req.bucket
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Ok(OutputChunk { data: payload.into_bytes() })).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn connect_with_retry(addr: &str) -> WorkerRegistryClient<Channel> {
    loop {
        match WorkerRegistryClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(e) => {
                eprintln!("scheduler not reachable yet ({e}); retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler_addr =
        std::env::var("SCHEDULER_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let listen_str =
        std::env::var("WORKER_LISTEN").unwrap_or_else(|_| "127.0.0.1:50061".to_string());
    let listen: SocketAddr = listen_str.parse()?;
    let advertise =
        std::env::var("WORKER_ADVERTISE").unwrap_or_else(|_| format!("http://{listen_str}"));
    let slots: u32 = std::env::var("WORKER_SLOTS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    let delay_ms: u64 =
        std::env::var("TASK_DELAY_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(100);

    // Start serving first so the scheduler can dial us as soon as we register.
    let worker = MockWorker { scheduler_addr: scheduler_addr.clone(), delay: Duration::from_millis(delay_ms) };
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(WorkerServiceServer::new(worker))
            .serve(listen)
            .await
        {
            eprintln!("worker server error: {e}");
        }
    });

    let mut client = connect_with_retry(&scheduler_addr).await;
    let resp = client
        .register_worker(RegisterWorkerRequest {
            address: advertise.clone(),
            resources: Some(WorkerResources { slots, memory_bytes: 0 }),
        })
        .await?
        .into_inner();
    eprintln!("registered as {} (advertising {advertise})", resp.worker_id);

    let interval = Duration::from_millis(resp.heartbeat_interval_ms as u64);
    let worker_id = resp.worker_id;
    loop {
        tokio::time::sleep(interval).await;
        let hb = HeartbeatRequest { worker_id: worker_id.clone(), free_slots: slots };
        match client.heartbeat(hb).await {
            Ok(r) => {
                if !r.get_ref().known {
                    eprintln!("scheduler no longer knows this worker; exiting so it can be restarted");
                    return Ok(());
                }
            }
            Err(e) => eprintln!("heartbeat failed: {e}"),
        }
    }
}
