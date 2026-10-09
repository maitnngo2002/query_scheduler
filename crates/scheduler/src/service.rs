//! gRPC service implementations. Thin wrappers over `Engine`.

use std::sync::Arc;
use std::time::Duration;

use fragmenter::tasks::TaskId;
use scheduler_proto::v1::scheduler_service_server::SchedulerService;
use scheduler_proto::v1::worker_registry_server::WorkerRegistry;
use scheduler_proto::v1::worker_service_client::WorkerServiceClient;
use scheduler_proto::v1::{
    CancelQueryRequest, CancelQueryResponse, FetchResultsRequest, FetchTaskOutputRequest,
    GetQueryMetricsRequest,
    GetQueryMetricsResponse, GetQueryStatusRequest, GetQueryStatusResponse, HeartbeatRequest,
    HeartbeatResponse, RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse, ResultBatch, SubmitQueryRequest, SubmitQueryResponse,
    TaskId as ProtoTaskId, TaskState, TaskStatus,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::engine::{Engine, ResultsError, SubmitError};
use crate::execution::TaskPhase;
use crate::queries::QueryError;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);

fn to_status(e: QueryError) -> Status {
    match e {
        QueryError::NotFound => Status::not_found("unknown query id"),
        QueryError::InvalidTransition { from, to } => Status::failed_precondition(format!(
            "invalid state transition {:?} -> {:?}",
            from, to
        )),
    }
}

fn phase_to_proto(phase: &TaskPhase) -> (TaskState, String) {
    match phase {
        TaskPhase::Pending => (TaskState::Pending, String::new()),
        TaskPhase::Dispatched { worker_id, .. } => (TaskState::Running, worker_id.clone()),
        TaskPhase::Done { worker_id, .. } => (TaskState::Succeeded, worker_id.clone()),
        TaskPhase::Failed(_) => (TaskState::Failed, String::new()),
        TaskPhase::Cancelled => (TaskState::Cancelled, String::new()),
    }
}

/// Streams bucket 0 of one task's output from its worker into `tx`.
/// Stops quietly if the client has gone away.
async fn stream_task_output(
    query_id: &str,
    task: TaskId,
    address: &str,
    tx: &mpsc::Sender<Result<ResultBatch, Status>>,
) -> Result<(), Status> {
    let mut client = WorkerServiceClient::connect(address.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("worker {address} is unreachable: {e}")))?;
    let request = FetchTaskOutputRequest {
        task: Some(ProtoTaskId {
            query_id: query_id.to_string(),
            fragment_id: task.fragment,
            partition: task.partition,
        }),
        bucket: 0,
    };
    let mut stream = client.fetch_task_output(request).await?.into_inner();
    while let Some(chunk) = stream.message().await? {
        if tx.send(Ok(ResultBatch { arrow_ipc: chunk.data })).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

pub struct SchedulerSvc {
    pub engine: Arc<Engine>,
}

#[tonic::async_trait]
impl SchedulerService for SchedulerSvc {
    async fn submit_query(
        &self,
        request: Request<SubmitQueryRequest>,
    ) -> Result<Response<SubmitQueryResponse>, Status> {
        let req = request.into_inner();
        let priority = req.options.map(|o| o.priority).unwrap_or(0);
        let submitted = match (&req.distributed, req.plan.is_empty()) {
            (Some(_), false) => {
                return Err(Status::invalid_argument("set either plan or distributed, not both"))
            }
            (Some(query), true) => self.engine.submit_distributed(query, priority),
            (None, true) => return Err(Status::invalid_argument("plan must not be empty")),
            (None, false) => self.engine.submit(&req.plan, priority),
        };
        let query_id = submitted.map_err(|e| match e {
            SubmitError::BadPlan(msg) => Status::invalid_argument(msg),
        })?;
        Ok(Response::new(SubmitQueryResponse { query_id }))
    }

    async fn get_query_status(
        &self,
        request: Request<GetQueryStatusRequest>,
    ) -> Result<Response<GetQueryStatusResponse>, Status> {
        let query_id = request.into_inner().query_id;
        let state = self.engine.queries.status(&query_id).map_err(to_status)?;
        let tasks = self
            .engine
            .task_phases(&query_id)
            .into_iter()
            .map(|(id, phase)| {
                let (state, worker_id) = phase_to_proto(&phase);
                TaskStatus {
                    task: Some(ProtoTaskId {
                        query_id: query_id.clone(),
                        fragment_id: id.fragment,
                        partition: id.partition,
                    }),
                    state: state as i32,
                    worker_id,
                }
            })
            .collect();
        let error_message = self.engine.failure(&query_id).unwrap_or_default();
        Ok(Response::new(GetQueryStatusResponse {
            query_id,
            state: state as i32,
            tasks,
            error_message,
        }))
    }

    async fn get_query_metrics(
        &self,
        request: Request<GetQueryMetricsRequest>,
    ) -> Result<Response<GetQueryMetricsResponse>, Status> {
        let query_id = request.into_inner().query_id;
        // Validates that the query exists; per-task metrics arrive in a later phase.
        self.engine.queries.status(&query_id).map_err(to_status)?;
        Ok(Response::new(GetQueryMetricsResponse { query_id, tasks: vec![] }))
    }

    type FetchResultsStream = ReceiverStream<Result<ResultBatch, Status>>;

    async fn fetch_results(
        &self,
        request: Request<FetchResultsRequest>,
    ) -> Result<Response<Self::FetchResultsStream>, Status> {
        let query_id = request.into_inner().query_id;
        let sources = self.engine.result_sources(&query_id).map_err(|e| match e {
            ResultsError::NotFound => Status::not_found("unknown query id"),
            ResultsError::NotReady(msg) => Status::failed_precondition(msg),
        })?;

        // Pull each root task's output from the worker that holds it and
        // forward the chunks to the client, in partition order.
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            for (task, address) in sources {
                if let Err(status) = stream_task_output(&query_id, task, &address, &tx).await {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn cancel_query(
        &self,
        request: Request<CancelQueryRequest>,
    ) -> Result<Response<CancelQueryResponse>, Status> {
        let query_id = request.into_inner().query_id;
        self.engine.cancel(&query_id).map_err(to_status)?;
        Ok(Response::new(CancelQueryResponse {}))
    }
}

pub struct RegistrySvc {
    pub engine: Arc<Engine>,
}

#[tonic::async_trait]
impl WorkerRegistry for RegistrySvc {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        if req.address.is_empty() {
            return Err(Status::invalid_argument("worker address must not be empty"));
        }
        let slots = req.resources.map(|r| r.slots).unwrap_or(1);
        let worker_id = self.engine.workers.register(req.address, slots);
        eprintln!("registered worker {worker_id} with {slots} slots");
        self.engine.wake();
        Ok(Response::new(RegisterWorkerResponse {
            worker_id,
            heartbeat_interval_ms: HEARTBEAT_INTERVAL.as_millis() as u32,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let known = self.engine.workers.heartbeat(&req.worker_id, req.free_slots);
        Ok(Response::new(HeartbeatResponse { known }))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let task = req
            .task
            .ok_or_else(|| Status::invalid_argument("task is required"))?;
        let state = TaskState::try_from(req.state).unwrap_or(TaskState::Unspecified);
        self.engine.on_task_report(
            &task.query_id,
            TaskId { fragment: task.fragment_id, partition: task.partition },
            state,
            &req.error_message,
        );
        Ok(Response::new(ReportTaskStatusResponse {}))
    }
}
