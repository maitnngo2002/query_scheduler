//! gRPC service implementations (Phase 1: skeleton behavior).

use std::sync::Arc;
use std::time::Duration;

use scheduler_proto::v1::scheduler_service_server::SchedulerService;
use scheduler_proto::v1::worker_registry_server::WorkerRegistry;
use scheduler_proto::v1::{
    CancelQueryRequest, CancelQueryResponse, FetchResultsRequest, GetQueryStatusRequest,
    GetQueryStatusResponse, HeartbeatRequest, HeartbeatResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportTaskStatusRequest, ReportTaskStatusResponse, ResultBatch,
    SubmitQueryRequest, SubmitQueryResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::queries::{QueryError, QueryManager};
use crate::workers::WorkerManager;

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

pub struct SchedulerSvc {
    pub queries: Arc<QueryManager>,
}

#[tonic::async_trait]
impl SchedulerService for SchedulerSvc {
    async fn submit_query(
        &self,
        request: Request<SubmitQueryRequest>,
    ) -> Result<Response<SubmitQueryResponse>, Status> {
        let req = request.into_inner();
        if req.plan.is_empty() {
            return Err(Status::invalid_argument("plan must not be empty"));
        }
        let priority = req.options.map(|o| o.priority).unwrap_or(0);
        // TODO(phase 2): decode plan -> fragment() -> enqueue ready fragments.
        let query_id = self.queries.submit(priority);
        Ok(Response::new(SubmitQueryResponse { query_id }))
    }

    async fn get_query_status(
        &self,
        request: Request<GetQueryStatusRequest>,
    ) -> Result<Response<GetQueryStatusResponse>, Status> {
        let query_id = request.into_inner().query_id;
        let state = self.queries.status(&query_id).map_err(to_status)?;
        Ok(Response::new(GetQueryStatusResponse {
            query_id,
            state: state as i32,
            fragments: vec![],
            error_message: String::new(),
        }))
    }

    type FetchResultsStream = ReceiverStream<Result<ResultBatch, Status>>;

    async fn fetch_results(
        &self,
        _request: Request<FetchResultsRequest>,
    ) -> Result<Response<Self::FetchResultsStream>, Status> {
        Err(Status::unimplemented("FetchResults is not implemented yet"))
    }

    async fn cancel_query(
        &self,
        request: Request<CancelQueryRequest>,
    ) -> Result<Response<CancelQueryResponse>, Status> {
        let query_id = request.into_inner().query_id;
        self.queries.cancel(&query_id).map_err(to_status)?;
        Ok(Response::new(CancelQueryResponse {}))
    }
}

pub struct RegistrySvc {
    pub workers: Arc<WorkerManager>,
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
        let worker_id = self.workers.register(req.address, slots);
        eprintln!("registered worker {worker_id} with {slots} slots");
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
        let known = self.workers.heartbeat(&req.worker_id, req.free_slots);
        Ok(Response::new(HeartbeatResponse { known }))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let req = request.into_inner();
        // TODO(phase 2): feed into the query state manager.
        eprintln!(
            "task status: query={} fragment={} state={}",
            req.query_id, req.fragment_id, req.state
        );
        Ok(Response::new(ReportTaskStatusResponse {}))
    }
}
