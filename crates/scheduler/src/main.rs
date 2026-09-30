mod queries;
mod service;
mod workers;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scheduler_proto::v1::scheduler_service_server::SchedulerServiceServer;
use scheduler_proto::v1::worker_registry_server::WorkerRegistryServer;
use tonic::transport::Server;

use crate::queries::QueryManager;
use crate::service::{RegistrySvc, SchedulerSvc};
use crate::workers::WorkerManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("SCHEDULER_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    let workers = Arc::new(WorkerManager::new());
    let queries = Arc::new(QueryManager::new());

    // Periodically evict workers that stopped sending heartbeats.
    {
        let workers = workers.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                for id in workers.evict_stale(Duration::from_secs(15)) {
                    eprintln!("evicted stale worker {id}");
                }
            }
        });
    }

    eprintln!("scheduler listening on {addr}");
    Server::builder()
        .add_service(SchedulerServiceServer::new(SchedulerSvc { queries }))
        .add_service(WorkerRegistryServer::new(RegistrySvc { workers }))
        .serve(addr)
        .await?;
    Ok(())
}
