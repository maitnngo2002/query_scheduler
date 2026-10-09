//! Worker tests. A stand-in scheduler (it only records status reports) dispatches the
//! fragments of a real query by hand to two real workers over gRPC. The workers decode,
//! run, store, and serve; the test then fetches the final result from a worker and
//! compares it with plain single-node DataFusion.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use df_adapter::codec::{encode_plan, ShuffleCodec};
use df_adapter::example;
use df_adapter::rewrite::distribute;
use df_adapter::store::{decode_batches, ShuffleStore};
use scheduler_proto::v1::worker_registry_server::{WorkerRegistry, WorkerRegistryServer};
use scheduler_proto::v1::worker_service_client::WorkerServiceClient;
use scheduler_proto::v1::{
    ExecuteTaskRequest, FetchTaskOutputRequest, HeartbeatRequest, HeartbeatResponse,
    ProducerLocation, RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse, TaskId, TaskState,
};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use worker::Worker;

/// Records every task status report it receives.
#[derive(Clone, Default)]
struct FakeScheduler {
    reports: Arc<Mutex<Vec<ReportTaskStatusRequest>>>,
}

#[tonic::async_trait]
impl WorkerRegistry for FakeScheduler {
    async fn register_worker(
        &self,
        _request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        Ok(Response::new(RegisterWorkerResponse {
            worker_id: "w-test".to_string(),
            heartbeat_interval_ms: 1000,
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse { known: true }))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        self.reports.lock().unwrap().push(request.into_inner());
        Ok(Response::new(ReportTaskStatusResponse {}))
    }
}

/// A free local port that is never handed out twice in this process.
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

async fn wait_until_up(url: &str) {
    for _ in 0..100 {
        if WorkerServiceClient::connect(url.to_string()).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{url} never came up");
}

async fn start_scheduler(fake: FakeScheduler) -> String {
    let addr = free_addr();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerRegistryServer::new(fake))
            .serve(addr)
            .await
            .unwrap();
    });
    let url = format!("http://{addr}");
    // The registry service has no WorkerService, so wait by trying a plain connection.
    for _ in 0..100 {
        if scheduler_proto::v1::worker_registry_client::WorkerRegistryClient::connect(url.clone())
            .await
            .is_ok()
        {
            return url;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("fake scheduler never came up");
}

async fn start_worker(scheduler_url: &str) -> String {
    let addr = free_addr();
    let url = format!("http://{addr}");
    let worker = Worker::new(scheduler_url.to_string(), url.clone());
    tokio::spawn(async move {
        worker::serve(addr, worker).await.unwrap();
    });
    wait_until_up(&url).await;
    url
}

async fn wait_for_reports(fake: &FakeScheduler, count: usize) {
    for _ in 0..1500 {
        if fake.reports.lock().unwrap().len() >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {count} task reports; got {}",
        fake.reports.lock().unwrap().len()
    );
}

fn render(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

#[tokio::test]
async fn real_workers_run_a_query_dispatched_by_hand() {
    let ctx = example::context().await.unwrap();
    let df = ctx.sql(example::QUERY).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let expected = df.collect().await.unwrap();

    // The plan is built once with a placeholder query id; workers override it.
    let scratch = Arc::new(ShuffleStore::new());
    let distributed = distribute(&plan, "placeholder", &scratch).unwrap();

    let fake = FakeScheduler::default();
    let scheduler_url = start_scheduler(fake.clone()).await;
    let worker_urls = [start_worker(&scheduler_url).await, start_worker(&scheduler_url).await];
    let worker_of = |fragment: usize, task: usize| (fragment + task) % 2;
    let query_id = "q-real";

    // Where each producer task will run (the real scheduler learns this as it dispatches).
    let mut locations: HashMap<(usize, usize), String> = HashMap::new();
    for fragment in &distributed.fragments {
        for task in 0..fragment.info.tasks {
            locations.insert((fragment.info.id, task), worker_urls[worker_of(fragment.info.id, task)].clone());
        }
    }

    // Act as the scheduler: dispatch fragment by fragment, waiting for each stage to finish.
    let mut dispatched = 0;
    for fragment in &distributed.fragments {
        let plan_bytes = encode_plan(&fragment.plan, &ShuffleCodec::new(Arc::clone(&scratch))).unwrap();

        let mut inputs = Vec::new();
        for &input in &fragment.info.inputs {
            for producer in 0..distributed.fragments[input].info.tasks {
                inputs.push(ProducerLocation {
                    fragment_id: input as u32,
                    producer_partition: producer as u32,
                    worker_address: locations[&(input, producer)].clone(),
                    bucket: 0,
                });
            }
        }

        for task in 0..fragment.info.tasks {
            let mut client = WorkerServiceClient::connect(worker_urls[worker_of(fragment.info.id, task)].clone())
                .await
                .unwrap();
            let response = client
                .execute_task(ExecuteTaskRequest {
                    task: Some(TaskId {
                        query_id: query_id.to_string(),
                        fragment_id: fragment.info.id as u32,
                        partition: task as u32,
                    }),
                    fragment_plan: plan_bytes.clone(),
                    output_partitions: fragment.info.output_buckets() as u32,
                    inputs: inputs.clone(),
                })
                .await
                .unwrap()
                .into_inner();
            assert!(response.accepted);
            dispatched += 1;
        }
        wait_for_reports(&fake, dispatched).await;
    }

    // Every task reported success, with metrics.
    let reports = fake.reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 15);
    for report in &reports {
        assert_eq!(report.state, TaskState::Succeeded as i32, "{}", report.error_message);
        let metrics = report.metrics.as_ref().unwrap();
        assert!(metrics.end_unix_ms >= metrics.start_unix_ms);
    }

    // Fetch the final result from the worker holding the root task's output.
    let root = distributed.fragments.last().unwrap();
    let mut actual: Vec<RecordBatch> = Vec::new();
    for task in 0..root.info.tasks {
        let mut client = WorkerServiceClient::connect(worker_urls[worker_of(root.info.id, task)].clone())
            .await
            .unwrap();
        let mut stream = client
            .fetch_task_output(FetchTaskOutputRequest {
                task: Some(TaskId {
                    query_id: query_id.to_string(),
                    fragment_id: root.info.id as u32,
                    partition: task as u32,
                }),
                bucket: 0,
            })
            .await
            .unwrap()
            .into_inner();
        while let Some(chunk) = stream.message().await.unwrap() {
            actual.extend(decode_batches(&chunk.data).unwrap());
        }
    }
    assert_eq!(render(&actual), render(&expected));

    // The root task reported the three result rows.
    let root_report = reports
        .iter()
        .find(|r| r.task.as_ref().map(|t| t.fragment_id as usize) == Some(root.info.id))
        .unwrap();
    assert_eq!(root_report.metrics.as_ref().unwrap().output_rows, 3);
}

#[tokio::test]
async fn a_task_with_a_bad_plan_reports_failure() {
    let fake = FakeScheduler::default();
    let scheduler_url = start_scheduler(fake.clone()).await;
    let worker_url = start_worker(&scheduler_url).await;

    let mut client = WorkerServiceClient::connect(worker_url).await.unwrap();
    let response = client
        .execute_task(ExecuteTaskRequest {
            task: Some(TaskId { query_id: "q-bad".to_string(), fragment_id: 0, partition: 0 }),
            fragment_plan: b"this is not a plan".to_vec(),
            output_partitions: 1,
            inputs: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.accepted, "the worker accepts first and reports the failure afterwards");

    wait_for_reports(&fake, 1).await;
    let report = fake.reports.lock().unwrap()[0].clone();
    assert_eq!(report.state, TaskState::Failed as i32);
    assert!(!report.error_message.is_empty());
}

#[tokio::test]
async fn an_empty_plan_is_rejected_up_front() {
    let fake = FakeScheduler::default();
    let scheduler_url = start_scheduler(fake).await;
    let worker_url = start_worker(&scheduler_url).await;

    let mut client = WorkerServiceClient::connect(worker_url).await.unwrap();
    let err = client
        .execute_task(ExecuteTaskRequest {
            task: Some(TaskId { query_id: "q".to_string(), fragment_id: 0, partition: 0 }),
            fragment_plan: vec![],
            output_partitions: 1,
            inputs: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
