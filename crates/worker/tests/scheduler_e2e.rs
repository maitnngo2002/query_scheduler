//! Full-stack test: a real scheduler and two real workers, all in this process on
//! local ports. The client plans a query with DataFusion, cuts it into fragments,
//! submits it as a `DistributedQuery`, waits for it to finish, fetches the result
//! through the scheduler, and compares it with plain single-node DataFusion.
//!
//! It lives in the worker crate because it needs DataFusion; the scheduler crate
//! stays free of it and is only a dev-dependency here.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use df_adapter::example;
use df_adapter::rewrite::distribute;
use df_adapter::store::{decode_batches, ShuffleStore};
use df_adapter::submit::distributed_query;
use fragmenter::tasks::ExpandConfig;
use scheduler::engine::Engine;
use scheduler_proto::v1::scheduler_service_client::SchedulerServiceClient;
use scheduler_proto::v1::{
    FetchResultsRequest, GetQueryStatusRequest, GetQueryStatusResponse, QueryState,
    SubmitQueryRequest, TaskState,
};
use tonic::transport::Channel;
use worker::Worker;

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

/// Starts a real scheduler (gRPC services plus the dispatcher loop).
async fn start_scheduler() -> (String, Arc<Engine>) {
    let addr = free_addr();
    // The expand config only applies to JSON plans; a DistributedQuery carries its own task counts.
    let engine = Arc::new(Engine::new(ExpandConfig::default()));
    tokio::spawn(engine.clone().run_dispatcher());
    tokio::spawn(scheduler::serve(addr, engine.clone()));
    (format!("http://{addr}"), engine)
}

/// Starts a real worker that registers with the scheduler and heartbeats.
async fn start_worker(scheduler_url: &str, slots: u32) -> String {
    let addr = free_addr();
    let url = format!("http://{addr}");
    let worker = Worker::new(scheduler_url.to_string(), url.clone());
    tokio::spawn(async move {
        worker::serve(addr, worker).await.unwrap();
    });
    tokio::spawn(worker::register_and_heartbeat(scheduler_url.to_string(), url.clone(), slots));
    url
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

async fn wait_for_workers(engine: &Engine, count: usize) {
    for _ in 0..200 {
        if engine.workers.live_workers().len() >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("only {} of {count} workers registered", engine.workers.live_workers().len());
}

/// Polls until the query leaves QUEUED/RUNNING. Panics on timeout.
async fn wait_until_finished(
    client: &mut SchedulerServiceClient<Channel>,
    query_id: &str,
) -> GetQueryStatusResponse {
    for _ in 0..1200 {
        let status = client
            .get_query_status(GetQueryStatusRequest { query_id: query_id.to_string() })
            .await
            .unwrap()
            .into_inner();
        if status.state != QueryState::Queued as i32 && status.state != QueryState::Running as i32 {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("query {query_id} did not finish within 60 seconds");
}

fn render(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

/// Plans `sql`, runs it through the scheduler and workers, and requires the same
/// output as single-node DataFusion. Returns the final status for further checks.
async fn assert_cluster_matches(
    client: &mut SchedulerServiceClient<Channel>,
    sql: &str,
) -> GetQueryStatusResponse {
    let ctx = example::context().await.unwrap();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let expected = df.collect().await.unwrap();

    // Plan with a placeholder query id; workers replace it with the scheduler's id.
    let distributed = distribute(&plan, "placeholder", &Arc::new(ShuffleStore::new())).unwrap();
    let expected_tasks: u32 = distributed.fragments.iter().map(|f| f.info.tasks as u32).sum();
    let request = SubmitQueryRequest {
        plan: vec![],
        options: None,
        distributed: Some(distributed_query(&distributed).unwrap()),
    };
    let query_id = client.submit_query(request).await.unwrap().into_inner().query_id;

    let status = wait_until_finished(client, &query_id).await;
    assert_eq!(
        status.state,
        QueryState::Succeeded as i32,
        "query failed for {sql}: {}",
        status.error_message
    );
    assert_eq!(status.tasks.len() as u32, expected_tasks, "{sql}");
    assert!(status.tasks.iter().all(|t| t.state == TaskState::Succeeded as i32), "{sql}");

    let mut stream = client
        .fetch_results(FetchResultsRequest { query_id: query_id.clone() })
        .await
        .unwrap()
        .into_inner();
    let mut actual: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.message().await.unwrap() {
        actual.extend(decode_batches(&batch.arrow_ipc).unwrap());
    }
    assert_eq!(render(&actual), render(&expected), "results differ for: {sql}");
    status
}

#[tokio::test]
async fn scheduler_and_two_workers_match_single_node_datafusion() {
    let (scheduler_url, engine) = start_scheduler().await;
    let mut client = connect_scheduler(&scheduler_url).await;
    // Two slots each, so a four-task stage needs both workers.
    start_worker(&scheduler_url, 2).await;
    start_worker(&scheduler_url, 2).await;
    wait_for_workers(&engine, 2).await;

    // The example query: 6 fragments, 15 tasks, a partitioned join and a merge.
    let status = assert_cluster_matches(&mut client, example::QUERY).await;
    assert_eq!(status.tasks.len(), 15);
    let mut tasks_per_worker: HashMap<String, usize> = HashMap::new();
    for t in &status.tasks {
        *tasks_per_worker.entry(t.worker_id.clone()).or_default() += 1;
    }
    assert_eq!(tasks_per_worker.len(), 2, "both workers should run tasks: {tasks_per_worker:?}");

    // More queries on the same cluster; each gets its own query id from the scheduler.
    assert_cluster_matches(
        &mut client,
        "SELECT segment, COUNT(*) AS n, MIN(id) AS lo FROM customers GROUP BY segment ORDER BY segment",
    )
    .await;
    assert_cluster_matches(
        &mut client,
        "SELECT order_id, total FROM orders WHERE cust_id = 7 ORDER BY order_id",
    )
    .await;
}
