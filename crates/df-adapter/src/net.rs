//! Network shuffle: serve a worker's buckets over gRPC and fetch buckets from other workers.
//!
//! * [`BucketServer`] implements the worker-side `FetchTaskOutput` call by streaming
//!   one bucket out of a [`ShuffleStore`]. Each `OutputChunk` holds one batch as a
//!   complete Arrow IPC stream; an empty bucket sends no chunks.
//! * [`RoutedSource`] is a [`BucketSource`] for readers. It knows which worker ran
//!   each producer task. Buckets that live on this worker are read locally; the
//!   rest are fetched from the owning worker.
//!
//! The real worker will serve its store with `BucketServer` and build a
//! `RoutedSource` from the producer locations the scheduler sends with each task.
//!
//! Limits for now: a new connection per fetch, and one batch per gRPC message, so a
//! batch larger than tonic's default 4 MB message limit would fail.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use futures::future::BoxFuture;
use scheduler_proto::v1::worker_service_client::WorkerServiceClient;
use scheduler_proto::v1::worker_service_server::WorkerService;
use scheduler_proto::v1::{
    ExecuteTaskRequest, ExecuteTaskResponse, FetchTaskOutputRequest, OutputChunk, TaskId,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::store::{decode_batches, encode_batches, BucketSource, OutputKey, ShuffleStore};

/// Serves buckets from a store through the `FetchTaskOutput` call.
/// `ExecuteTask` is not supported: running tasks is the real worker's job.
#[derive(Debug)]
pub struct BucketServer {
    store: Arc<ShuffleStore>,
    served: Arc<AtomicUsize>,
}

impl BucketServer {
    pub fn new(store: Arc<ShuffleStore>) -> Self {
        BucketServer { store, served: Arc::new(AtomicUsize::new(0)) }
    }

    /// Counts successful `FetchTaskOutput` requests; clone it before moving the server.
    pub fn served_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.served)
    }
}

#[tonic::async_trait]
impl WorkerService for BucketServer {
    async fn execute_task(
        &self,
        _request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        Err(Status::unimplemented("this server only serves shuffle output"))
    }

    type FetchTaskOutputStream = ReceiverStream<Result<OutputChunk, Status>>;

    async fn fetch_task_output(
        &self,
        request: Request<FetchTaskOutputRequest>,
    ) -> Result<Response<Self::FetchTaskOutputStream>, Status> {
        let req = request.into_inner();
        let task = req.task.ok_or_else(|| Status::invalid_argument("task is required"))?;
        let key = OutputKey {
            query_id: task.query_id,
            fragment: task.fragment_id as usize,
            task: task.partition as usize,
            bucket: req.bucket as usize,
        };
        let batches = self
            .store
            .get(&key)
            .ok_or_else(|| Status::not_found(format!("no shuffle output for {key:?}")))?;
        self.served.fetch_add(1, Ordering::SeqCst);

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for batch in batches {
                let chunk = match encode_batches(&batch.schema(), std::slice::from_ref(&batch)) {
                    Ok(data) => Ok(OutputChunk { data }),
                    Err(e) => Err(Status::internal(e.to_string())),
                };
                let failed = chunk.is_err();
                if tx.send(chunk).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn external(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

async fn fetch_remote(address: String, key: OutputKey) -> DfResult<Vec<RecordBatch>> {
    let mut client = WorkerServiceClient::connect(address).await.map_err(external)?;
    let request = FetchTaskOutputRequest {
        task: Some(TaskId {
            query_id: key.query_id.clone(),
            fragment_id: key.fragment as u32,
            partition: key.task as u32,
        }),
        bucket: key.bucket as u32,
    };
    let mut stream = client.fetch_task_output(request).await.map_err(external)?.into_inner();
    let mut batches = Vec::new();
    while let Some(chunk) = stream.message().await.map_err(external)? {
        batches.extend(decode_batches(&chunk.data)?);
    }
    Ok(batches)
}

/// Fetches a bucket from the worker that ran its producer task, or from the local
/// store when that worker is this one.
#[derive(Debug, Clone)]
pub struct RoutedSource {
    local_address: Option<String>,
    local: Arc<ShuffleStore>,
    /// (fragment, producer task) -> address of the worker that ran it.
    locations: Arc<HashMap<(usize, usize), String>>,
}

impl RoutedSource {
    pub fn new(
        local_address: Option<String>,
        local: Arc<ShuffleStore>,
        locations: HashMap<(usize, usize), String>,
    ) -> Self {
        RoutedSource { local_address, local, locations: Arc::new(locations) }
    }
}

impl BucketSource for RoutedSource {
    fn fetch(&self, key: OutputKey) -> BoxFuture<'static, DfResult<Vec<RecordBatch>>> {
        match self.locations.get(&(key.fragment, key.task)).cloned() {
            None => Box::pin(futures::future::ready(Err(DataFusionError::Execution(format!(
                "no known location for fragment {} task {}",
                key.fragment, key.task
            ))))),
            Some(address) if self.local_address.as_deref() == Some(address.as_str()) => {
                self.local.fetch(key)
            }
            Some(address) => Box::pin(fetch_remote(address, key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use futures::TryStreamExt;
    use scheduler_proto::v1::worker_service_server::WorkerServiceServer;
    use tonic::transport::Server;

    use crate::codec::{decode_plan, encode_plan, ShuffleCodec};
    use crate::example;
    use crate::rewrite::distribute;

    /// A free local port that is never handed out twice in this process.
    fn free_addr() -> std::net::SocketAddr {
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

    /// Starts a bucket server over `store`; returns its URL and its served-request counter.
    async fn serve(store: Arc<ShuffleStore>) -> (String, Arc<AtomicUsize>) {
        let server = BucketServer::new(store);
        let counter = server.served_counter();
        let addr = free_addr();
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerServiceServer::new(server))
                .serve(addr)
                .await
                .unwrap();
        });
        let url = format!("http://{addr}");
        for _ in 0..100 {
            if WorkerServiceClient::connect(url.clone()).await.is_ok() {
                return (url, counter);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("bucket server at {url} never came up");
    }

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![("n", Arc::new(Int32Array::from(values)) as ArrayRef)])
            .unwrap()
    }

    fn key(fragment: usize, task: usize, bucket: usize) -> OutputKey {
        OutputKey { query_id: "q".to_string(), fragment, task, bucket }
    }

    fn render(batches: &[RecordBatch]) -> String {
        pretty_format_batches(batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn fetches_a_bucket_over_grpc() {
        let store = Arc::new(ShuffleStore::new());
        store.put(key(1, 0, 2), vec![batch(vec![1, 2]), batch(vec![3])]);
        let (url, counter) = serve(Arc::clone(&store)).await;

        let source = RoutedSource::new(
            None,
            Arc::new(ShuffleStore::new()),
            HashMap::from([((1, 0), url)]),
        );
        let got = source.fetch(key(1, 0, 2)).await.unwrap();
        assert_eq!(got, vec![batch(vec![1, 2]), batch(vec![3])]);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_empty_bucket_comes_back_empty() {
        let store = Arc::new(ShuffleStore::new());
        store.put(key(1, 0, 0), vec![]);
        let (url, _) = serve(Arc::clone(&store)).await;
        let source = RoutedSource::new(None, Arc::new(ShuffleStore::new()), HashMap::from([((1, 0), url)]));
        assert!(source.fetch(key(1, 0, 0)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_missing_bucket_is_an_error() {
        let (url, _) = serve(Arc::new(ShuffleStore::new())).await;
        let source = RoutedSource::new(None, Arc::new(ShuffleStore::new()), HashMap::from([((1, 0), url)]));
        assert!(source.fetch(key(1, 0, 0)).await.is_err());
    }

    #[tokio::test]
    async fn buckets_on_this_worker_are_read_locally() {
        // The address points nowhere; a network attempt would fail.
        let local = Arc::new(ShuffleStore::new());
        local.put(key(1, 0, 0), vec![batch(vec![7])]);
        let me = "http://127.0.0.1:1".to_string();
        let source = RoutedSource::new(Some(me.clone()), local, HashMap::from([((1, 0), me)]));
        assert_eq!(source.fetch(key(1, 0, 0)).await.unwrap(), vec![batch(vec![7])]);
    }

    #[tokio::test]
    async fn an_unknown_producer_location_is_an_error() {
        let source = RoutedSource::new(None, Arc::new(ShuffleStore::new()), HashMap::new());
        assert!(source.fetch(key(9, 9, 0)).await.is_err());
    }

    /// The key test. Two simulated workers, each with its own store and its own bucket server.
    /// Every task is decoded from bytes on the worker that runs it and reads its inputs through
    /// a router, so buckets produced on the other worker really cross gRPC.
    async fn assert_matches_across_two_workers(sql: &str) {
        let ctx = example::context().await.unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let expected = df.collect().await.unwrap();

        // The scheduler side builds the fragment plans once; workers only receive bytes.
        let scratch = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "q-net", &scratch).unwrap();

        let stores = [Arc::new(ShuffleStore::new()), Arc::new(ShuffleStore::new())];
        let (url_a, served_a) = serve(Arc::clone(&stores[0])).await;
        let (url_b, served_b) = serve(Arc::clone(&stores[1])).await;
        let addresses = [url_a, url_b];
        let worker_of = |fragment: usize, task: usize| (fragment + task) % 2;

        let mut locations = HashMap::new();
        for fragment in &distributed.fragments {
            for task in 0..fragment.info.tasks {
                locations.insert(
                    (fragment.info.id, task),
                    addresses[worker_of(fragment.info.id, task)].clone(),
                );
            }
        }

        let run = async {
            let task_ctx = ctx.task_ctx();
            let root = distributed.fragments.len() - 1;
            let mut result: Vec<RecordBatch> = Vec::new();
            for fragment in &distributed.fragments {
                let bytes = encode_plan(&fragment.plan, &ShuffleCodec::new(Arc::clone(&scratch))).unwrap();
                for task in 0..fragment.info.tasks {
                    let w = worker_of(fragment.info.id, task);
                    let source = Arc::new(RoutedSource::new(
                        Some(addresses[w].clone()),
                        Arc::clone(&stores[w]),
                        locations.clone(),
                    ));
                    let codec = ShuffleCodec::with_source(Arc::clone(&stores[w]), source);
                    let task_plan = decode_plan(&bytes, &task_ctx, &codec).unwrap();
                    let batches: Vec<RecordBatch> = task_plan
                        .execute(task, Arc::clone(&task_ctx))
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    if fragment.info.id == root {
                        result.extend(batches);
                    }
                }
            }
            result
        };
        let actual = tokio::time::timeout(Duration::from_secs(120), run)
            .await
            .unwrap_or_else(|_| panic!("cross-worker run timed out: {sql}"));

        assert_eq!(render(&actual), render(&expected), "results differ for: {sql}");
        assert!(!stores[0].is_empty() && !stores[1].is_empty(), "both workers should hold output");
        let fetched = served_a.load(Ordering::SeqCst) + served_b.load(Ordering::SeqCst);
        assert!(fetched > 0, "buckets should have crossed the network");
    }

    #[tokio::test]
    async fn example_query_runs_across_two_workers() {
        assert_matches_across_two_workers(example::QUERY).await;
    }

    #[tokio::test]
    async fn grouped_aggregate_runs_across_two_workers() {
        assert_matches_across_two_workers(
            "SELECT segment, COUNT(*) AS n, MIN(id) AS lo FROM customers GROUP BY segment ORDER BY segment",
        )
        .await;
    }
}
