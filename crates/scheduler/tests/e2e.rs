//! End-to-end tests: a real scheduler and real gRPC workers, all in this process
//! on local ports. Workers are defined here (not the `mock-worker` binary) so the
//! tests can record exactly what the scheduler sends and inject failures.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use fragmenter::tasks::ExpandConfig;
use scheduler::engine::Engine;
use scheduler_proto::v1::scheduler_service_client::SchedulerServiceClient;
use scheduler_proto::v1::worker_registry_client::WorkerRegistryClient;
use scheduler_proto::v1::worker_service_server::{WorkerService, WorkerServiceServer};
use scheduler_proto::v1::{
    DistributedFragment, DistributedQuery, ExecuteTaskRequest, ExecuteTaskResponse, FetchResultsRequest,
    FragmentInput, FetchTaskOutputRequest,
    GetQueryStatusRequest, GetQueryStatusResponse, OutputChunk, QueryState, RegisterWorkerRequest,
    ReportTaskStatusRequest, SubmitQueryRequest, TaskState, WorkerResources,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

const EXAMPLE_PLAN: &str = include_str!("../../../examples/join_agg.json");

#[derive(Clone, Copy)]
enum Behavior {
    Succeed,
    Fail,
}

struct TestWorker {
    scheduler_url: String,
    behavior: Behavior,
    received: Arc<Mutex<Vec<ExecuteTaskRequest>>>,
}

#[tonic::async_trait]
impl WorkerService for TestWorker {
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();
        self.received.lock().unwrap().push(req.clone());

        let scheduler_url = self.scheduler_url.clone();
        let behavior = self.behavior;
        let task = req.task.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let (state, error_message) = match behavior {
                Behavior::Succeed => (TaskState::Succeeded, String::new()),
                Behavior::Fail => (TaskState::Failed, "injected failure".to_string()),
            };
            let mut client = WorkerRegistryClient::connect(scheduler_url).await.unwrap();
            client
                .report_task_status(ReportTaskStatusRequest {
                    task,
                    state: state as i32,
                    error_message,
                    metrics: None,
                })
                .await
                .unwrap();
        });
        Ok(Response::new(ExecuteTaskResponse { accepted: true }))
    }

    type FetchTaskOutputStream = ReceiverStream<Result<OutputChunk, Status>>;

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

struct WorkerHandle {
    address: String,
    received: Arc<Mutex<Vec<ExecuteTaskRequest>>>,
}

impl WorkerHandle {
    fn received(&self) -> Vec<ExecuteTaskRequest> {
        self.received.lock().unwrap().clone()
    }
}

/// Picks a currently free local port and never hands out the same port twice in
/// this process, even across parallel tests. There is still a small race between
/// releasing the port here and binding it in the server, which is acceptable.
fn free_addr() -> SocketAddr {
    static USED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let used = USED.get_or_init(|| Mutex::new(HashSet::new()));
    loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        if used.lock().unwrap().insert(addr.port()) {
            return addr;
        }
    }
}

async fn start_scheduler() -> String {
    let addr = free_addr();
    let engine = Arc::new(Engine::new(ExpandConfig::default()));
    tokio::spawn(engine.clone().run_dispatcher());
    tokio::spawn(scheduler::serve(addr, engine));
    format!("http://{addr}")
}

async fn connect_scheduler(url: &str) -> SchedulerServiceClient<Channel> {
    for _ in 0..100 {
        if let Ok(client) = SchedulerServiceClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("scheduler at {url} never came up");
}

async fn connect_registry(url: &str) -> WorkerRegistryClient<Channel> {
    for _ in 0..100 {
        if let Ok(client) = WorkerRegistryClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("scheduler at {url} never came up");
}

async fn start_worker(scheduler_url: &str, behavior: Behavior, slots: u32) -> WorkerHandle {
    let addr = free_addr();
    let received = Arc::new(Mutex::new(Vec::new()));
    let worker = TestWorker {
        scheduler_url: scheduler_url.to_string(),
        behavior,
        received: received.clone(),
    };
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(worker))
            .serve(addr)
            .await
            .unwrap();
    });

    let address = format!("http://{addr}");
    let mut registry = connect_registry(scheduler_url).await;
    registry
        .register_worker(RegisterWorkerRequest {
            address: address.clone(),
            resources: Some(WorkerResources { slots, memory_bytes: 0 }),
        })
        .await
        .unwrap();
    WorkerHandle { address, received }
}

async fn submit_example(client: &mut SchedulerServiceClient<Channel>) -> String {
    client
        .submit_query(SubmitQueryRequest { plan: EXAMPLE_PLAN.as_bytes().to_vec(), options: None, distributed: None })
        .await
        .unwrap()
        .into_inner()
        .query_id
}

async fn wait_for_state(
    client: &mut SchedulerServiceClient<Channel>,
    query_id: &str,
    want: QueryState,
) -> GetQueryStatusResponse {
    for _ in 0..200 {
        let resp = client
            .get_query_status(GetQueryStatusRequest { query_id: query_id.to_string() })
            .await
            .unwrap()
            .into_inner();
        if resp.state == want as i32 {
            return resp;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("query {query_id} did not reach {want:?} in time");
}

#[tokio::test]
async fn query_runs_across_workers_and_returns_results() {
    let url = start_scheduler().await;
    let w1 = start_worker(&url, Behavior::Succeed, 2).await;
    let w2 = start_worker(&url, Behavior::Succeed, 2).await;
    let mut client = connect_scheduler(&url).await;

    let query_id = submit_example(&mut client).await;
    let status = wait_for_state(&mut client, &query_id, QueryState::Succeeded).await;

    // F0 1 + F1 1 + F2 4 + F3 4 + F4 1 tasks, all succeeded.
    assert_eq!(status.tasks.len(), 11);
    assert!(status.tasks.iter().all(|t| t.state == TaskState::Succeeded as i32));

    // Both workers took part.
    let (r1, r2) = (w1.received(), w2.received());
    assert!(!r1.is_empty() && !r2.is_empty(), "work should be spread across both workers");
    assert_eq!(r1.len() + r2.len(), 11);

    // Every task arrived with the inputs and output layout its fragment needs.
    // Input locations only exist once all producer tasks have finished, so this
    // also proves the stage barrier held.
    let worker_addresses = [w1.address.clone(), w2.address.clone()];
    for req in r1.iter().chain(r2.iter()) {
        let task = req.task.as_ref().unwrap();
        let (expected_inputs, expected_outputs) = match task.fragment_id {
            0 | 1 => (0, 4),
            2 => (2, 4),
            3 => (4, 1),
            4 => (4, 1),
            other => panic!("unexpected fragment {other}"),
        };
        assert_eq!(req.inputs.len(), expected_inputs, "inputs of fragment {}", task.fragment_id);
        assert_eq!(req.output_partitions, expected_outputs, "outputs of fragment {}", task.fragment_id);
        for input in &req.inputs {
            let expected_bucket = if task.fragment_id == 4 { 0 } else { task.partition };
            assert_eq!(input.bucket, expected_bucket);
            assert!(worker_addresses.contains(&input.worker_address));
        }
    }

    // Results come from the root task's worker.
    let mut stream = client
        .fetch_results(FetchResultsRequest { query_id: query_id.clone() })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(batch) = stream.message().await.unwrap() {
        bytes.extend(batch.arrow_ipc);
    }
    assert_eq!(String::from_utf8(bytes).unwrap(), format!("mock-result:{query_id}:4:0:0"));
}

#[tokio::test]
async fn a_failed_task_fails_the_query() {
    let url = start_scheduler().await;
    start_worker(&url, Behavior::Fail, 4).await;
    let mut client = connect_scheduler(&url).await;

    let query_id = submit_example(&mut client).await;
    let status = wait_for_state(&mut client, &query_id, QueryState::Failed).await;
    assert_eq!(status.error_message, "injected failure");

    // Results of a failed query are refused.
    let err = match client.fetch_results(FetchResultsRequest { query_id }).await {
        Err(status) => status,
        Ok(_) => panic!("fetch_results should fail for a failed query"),
    };
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn query_waits_for_a_worker_to_register() {
    let url = start_scheduler().await;
    let mut client = connect_scheduler(&url).await;

    let query_id = submit_example(&mut client).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let status = wait_for_state(&mut client, &query_id, QueryState::Queued).await;
    assert!(status.tasks.iter().all(|t| t.state == TaskState::Pending as i32));

    start_worker(&url, Behavior::Succeed, 4).await;
    wait_for_state(&mut client, &query_id, QueryState::Succeeded).await;
}

#[tokio::test]
async fn invalid_plans_are_rejected() {
    let url = start_scheduler().await;
    let mut client = connect_scheduler(&url).await;
    let err = match client
        .submit_query(SubmitQueryRequest { plan: b"not a plan".to_vec(), options: None, distributed: None })
        .await
    {
        Err(status) => status,
        Ok(_) => panic!("submit_query should reject garbage"),
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn inconsistent_distributed_queries_are_rejected() {
    let url = start_scheduler().await;
    let mut client = connect_scheduler(&url).await;
    let fragment = DistributedFragment { id: 0, tasks: 1, output_partitions: 1, inputs: vec![], plan: vec![1] };
    let bad_requests = [
        // No fragments.
        SubmitQueryRequest { plan: vec![], options: None, distributed: Some(DistributedQuery { fragments: vec![] }) },
        // Both plan forms at once.
        SubmitQueryRequest {
            plan: EXAMPLE_PLAN.as_bytes().to_vec(),
            options: None,
            distributed: Some(DistributedQuery { fragments: vec![fragment.clone()] }),
        },
        // A fragment reading one that does not precede it.
        SubmitQueryRequest {
            plan: vec![],
            options: None,
            distributed: Some(DistributedQuery {
                fragments: vec![DistributedFragment {
                    inputs: vec![FragmentInput { fragment_id: 0, partitioned: false }],
                    ..fragment
                }],
            }),
        },
    ];
    for request in bad_requests {
        let err = client.submit_query(request).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{}", err.message());
    }
}

#[tokio::test]
async fn results_of_an_unknown_query_are_not_found() {
    let url = start_scheduler().await;
    let mut client = connect_scheduler(&url).await;
    let err = match client.fetch_results(FetchResultsRequest { query_id: "q-404".to_string() }).await {
        Err(status) => status,
        Ok(_) => panic!("fetch_results should fail for an unknown query"),
    };
    assert_eq!(err.code(), tonic::Code::NotFound);
}
