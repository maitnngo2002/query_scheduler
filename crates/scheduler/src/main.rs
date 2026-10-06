use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fragmenter::tasks::ExpandConfig;
use scheduler::engine::Engine;

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("SCHEDULER_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    // Partitions per shuffle and scan splits per table. Per-table splits will
    // come from table metadata once a catalog is wired in.
    let cfg = ExpandConfig {
        shuffle_partitions: env_u32("SCHEDULER_SHUFFLE_PARTITIONS", 4),
        default_scan_splits: env_u32("SCHEDULER_SCAN_SPLITS", 1),
        scan_splits: HashMap::new(),
    };
    let engine = Arc::new(Engine::new(cfg));

    // Dispatch ready tasks to workers.
    tokio::spawn(engine.clone().run_dispatcher());

    // Evict workers that stopped sending heartbeats, and re-queue their in-flight tasks.
    {
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let evicted = engine.workers.evict_stale(Duration::from_secs(15));
                for id in &evicted {
                    eprintln!("evicted stale worker {id}");
                }
                engine.handle_evicted_workers(&evicted);
            }
        });
    }

    eprintln!("scheduler listening on {addr}");
    scheduler::serve(addr, engine).await?;
    Ok(())
}
