//! Worker binary.
//!
//! Env vars:
//!   SCHEDULER_ADDR    scheduler endpoint                    (default http://127.0.0.1:50051)
//!   WORKER_LISTEN     bind address                          (default 127.0.0.1:50061)
//!   WORKER_ADVERTISE  address others use to reach this one  (default http://<WORKER_LISTEN>)
//!   WORKER_SLOTS      concurrent tasks                      (default 4)

use std::net::SocketAddr;

use worker::{register_and_heartbeat, serve, Worker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler_url =
        std::env::var("SCHEDULER_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let listen_str =
        std::env::var("WORKER_LISTEN").unwrap_or_else(|_| "127.0.0.1:50061".to_string());
    let listen: SocketAddr = listen_str.parse()?;
    let advertise =
        std::env::var("WORKER_ADVERTISE").unwrap_or_else(|_| format!("http://{listen_str}"));
    let slots: u32 = std::env::var("WORKER_SLOTS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);

    let worker = Worker::new(scheduler_url.clone(), advertise.clone());
    tokio::spawn(register_and_heartbeat(scheduler_url, advertise, slots));

    eprintln!("worker listening on {listen}");
    serve(listen, worker).await?;
    Ok(())
}
