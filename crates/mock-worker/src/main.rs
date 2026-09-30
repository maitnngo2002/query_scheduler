//! Mock worker: registers with the scheduler, heartbeats, and "executes"
//! fragments by sleeping and then reporting success. Used to develop and test
//! the scheduler without a real execution engine.
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
    ExecuteFragmentRequest, ExecuteFragmentResponse, HeartbeatRequest, RegisterWorkerRequest,
    ReportTaskStatusRequest, TaskState, WorkerResources,
};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

struct MockWorker {
    scheduler_addr: String,
    delay: Duration,
}

#[tonic::async_trait]
impl WorkerService for MockWorker {
    async fn execute_fragment(
        &self,
        request: Request<ExecuteFragmentRequest>,
    ) -> Result<Response<ExecuteFragmentResponse>, Status> {
        let req = request.into_inner();
        let scheduler_addr = self.scheduler_addr.clone();
        let delay = self.delay;

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            match WorkerRegistryClient::connect(scheduler_addr).await {
                Ok(mut client) => {
                    let report = ReportTaskStatusRequest {
                        query_id: req.query_id,
                        fragment_id: req.fragment_id,
                        state: TaskState::Succeeded as i32,
                        error_message: String::new(),
                    };
                    if let Err(e) = client.report_task_status(report).await {
                        eprintln!("failed to report task status: {e}");
                    }
                }
                Err(e) => eprintln!("failed to connect to scheduler: {e}"),
            }
        });

        Ok(Response::new(ExecuteFragmentResponse { accepted: true }))
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
