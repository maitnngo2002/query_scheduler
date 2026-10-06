//! In-memory registry of worker nodes, with slot accounting.
//!
//! The scheduler is the source of truth for how many tasks each worker is
//! running (`in_flight`). Heartbeats only prove liveness.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub id: String,
    pub address: String,
    pub slots: u32,
    pub in_flight: u32,
    pub last_heartbeat: Instant,
}

#[derive(Default)]
pub struct WorkerManager {
    next_id: AtomicU64,
    workers: Mutex<HashMap<String, WorkerInfo>>,
}

impl WorkerManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, address: String, slots: u32) -> String {
        let id = format!("w-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let info = WorkerInfo {
            id: id.clone(),
            address,
            slots,
            in_flight: 0,
            last_heartbeat: Instant::now(),
        };
        self.workers.lock().unwrap().insert(id.clone(), info);
        id
    }

    /// Records liveness. Returns false if the worker is unknown (it should re-register).
    /// The worker's self-reported free slots are ignored; see the module docs.
    pub fn heartbeat(&self, worker_id: &str, _free_slots: u32) -> bool {
        let mut workers = self.workers.lock().unwrap();
        match workers.get_mut(worker_id) {
            Some(w) => {
                w.last_heartbeat = Instant::now();
                true
            }
            None => false,
        }
    }

    /// Reserves one slot on the least-loaded worker (lowest in_flight / slots),
    /// breaking ties by id. Returns a snapshot of that worker, or None if every
    /// worker is full or none are registered.
    pub fn acquire(&self) -> Option<WorkerInfo> {
        let mut workers = self.workers.lock().unwrap();
        let best = workers
            .values_mut()
            .filter(|w| w.in_flight < w.slots)
            .min_by(|a, b| {
                let left = a.in_flight as u64 * b.slots as u64;
                let right = b.in_flight as u64 * a.slots as u64;
                left.cmp(&right).then_with(|| a.id.cmp(&b.id))
            });
        let w = best?;
        w.in_flight += 1;
        Some(w.clone())
    }

    /// Frees a slot reserved by `acquire`. No-op for unknown (evicted) workers.
    pub fn release(&self, worker_id: &str) {
        if let Some(w) = self.workers.lock().unwrap().get_mut(worker_id) {
            w.in_flight = w.in_flight.saturating_sub(1);
        }
    }

    /// Removes workers whose last heartbeat is older than `timeout`.
    pub fn evict_stale(&self, timeout: Duration) -> Vec<String> {
        let mut workers = self.workers.lock().unwrap();
        let stale: Vec<String> = workers
            .values()
            .filter(|w| w.last_heartbeat.elapsed() > timeout)
            .map(|w| w.id.clone())
            .collect();
        for id in &stale {
            workers.remove(id);
        }
        stale
    }

    pub fn live_workers(&self) -> Vec<WorkerInfo> {
        self.workers.lock().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_heartbeat() {
        let m = WorkerManager::new();
        let id = m.register("http://127.0.0.1:1".to_string(), 4);
        assert_eq!(m.live_workers().len(), 1);
        assert!(m.heartbeat(&id, 2));
    }

    #[test]
    fn heartbeat_from_unknown_worker_is_rejected() {
        let m = WorkerManager::new();
        assert!(!m.heartbeat("w-404", 1));
    }

    #[test]
    fn ids_are_unique() {
        let m = WorkerManager::new();
        let a = m.register("a".to_string(), 1);
        let b = m.register("b".to_string(), 1);
        assert_ne!(a, b);
    }

    #[test]
    fn stale_workers_are_evicted() {
        let m = WorkerManager::new();
        let id = m.register("a".to_string(), 1);
        std::thread::sleep(Duration::from_millis(20));
        let evicted = m.evict_stale(Duration::from_millis(5));
        assert_eq!(evicted, vec![id]);
        assert!(m.live_workers().is_empty());
    }

    #[test]
    fn fresh_workers_are_kept() {
        let m = WorkerManager::new();
        m.register("a".to_string(), 1);
        assert!(m.evict_stale(Duration::from_secs(60)).is_empty());
        assert_eq!(m.live_workers().len(), 1);
    }

    #[test]
    fn acquire_spreads_load_and_respects_capacity() {
        let m = WorkerManager::new();
        let w0 = m.register("a".to_string(), 2);
        let w1 = m.register("b".to_string(), 2);

        let picked: Vec<String> = (0..4).map(|_| m.acquire().unwrap().id).collect();
        assert_eq!(picked, vec![w0.clone(), w1.clone(), w0.clone(), w1.clone()]);
        assert!(m.acquire().is_none(), "all slots are taken");

        m.release(&w1);
        assert_eq!(m.acquire().unwrap().id, w1);
    }

    #[test]
    fn acquire_with_no_workers_returns_none() {
        assert!(WorkerManager::new().acquire().is_none());
    }

    #[test]
    fn release_of_unknown_worker_is_a_no_op() {
        let m = WorkerManager::new();
        m.release("w-404");
    }
}
