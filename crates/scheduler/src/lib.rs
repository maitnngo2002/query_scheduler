//! Query scheduler library. The `scheduler` binary is a thin wrapper around it;
//! integration tests use it to start an in-process scheduler.

pub mod engine;
pub mod execution;
pub mod queries;
pub mod service;
pub mod workers;

use std::net::SocketAddr;
use std::sync::Arc;

use scheduler_proto::v1::scheduler_service_server::SchedulerServiceServer;
use scheduler_proto::v1::worker_registry_server::WorkerRegistryServer;
use tonic::transport::Server;

use crate::engine::Engine;
use crate::service::{RegistrySvc, SchedulerSvc};

/// Serves the client-facing and worker-facing gRPC APIs on `addr` until the
/// future is dropped. The caller is responsible for running
/// `Engine::run_dispatcher` separately.
pub async fn serve(addr: SocketAddr, engine: Arc<Engine>) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(SchedulerServiceServer::new(SchedulerSvc { engine: engine.clone() }))
        .add_service(WorkerRegistryServer::new(RegistrySvc { engine }))
        .serve(addr)
        .await
}
