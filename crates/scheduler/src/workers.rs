//! In-memory registry of worker nodes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub id: String,
    pub address: String,
    pub slots: u32,
    pub free_slots: u32,
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
            free_slots: slots,
            last_heartbeat: Instant::now(),
        };
        self.workers.lock().unwrap().insert(id.clone(), info);
        id
    }

    /// Returns false if the worker is unknown (it should re-register).
    pub fn heartbeat(&self, worker_id: &str, free_slots: u32) -> bool {
        let mut workers = self.workers.lock().unwrap();
        match workers.get_mut(worker_id) {
            Some(w) => {
                w.free_slots = free_slots;
                w.last_heartbeat = Instant::now();
                true
            }
            None => false,
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
        assert_eq!(m.live_workers()[0].free_slots, 2);
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
}
