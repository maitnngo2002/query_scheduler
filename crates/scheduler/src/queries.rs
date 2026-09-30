//! In-memory query state machine.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use scheduler_proto::v1::QueryState;

#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub state: QueryState,
    pub priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    NotFound,
    InvalidTransition { from: QueryState, to: QueryState },
}

#[derive(Default)]
pub struct QueryManager {
    next_id: AtomicU64,
    queries: Mutex<HashMap<String, QueryRecord>>,
}

fn is_valid_transition(from: QueryState, to: QueryState) -> bool {
    use QueryState::*;
    matches!(
        (from, to),
        (Queued, Running)
            | (Queued, Failed)
            | (Queued, Cancelled)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, Cancelled)
    )
}

impl QueryManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(&self, priority: i32) -> String {
        let id = format!("q-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        self.queries
            .lock()
            .unwrap()
            .insert(id.clone(), QueryRecord { state: QueryState::Queued, priority });
        id
    }

    pub fn status(&self, id: &str) -> Result<QueryState, QueryError> {
        self.queries
            .lock()
            .unwrap()
            .get(id)
            .map(|q| q.state)
            .ok_or(QueryError::NotFound)
    }

    pub fn transition(&self, id: &str, to: QueryState) -> Result<(), QueryError> {
        let mut queries = self.queries.lock().unwrap();
        let q = queries.get_mut(id).ok_or(QueryError::NotFound)?;
        if !is_valid_transition(q.state, to) {
            return Err(QueryError::InvalidTransition { from: q.state, to });
        }
        q.state = to;
        Ok(())
    }

    pub fn cancel(&self, id: &str) -> Result<(), QueryError> {
        self.transition(id, QueryState::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_queries_start_queued() {
        let m = QueryManager::new();
        let id = m.submit(0);
        assert_eq!(m.status(&id), Ok(QueryState::Queued));
    }

    #[test]
    fn happy_path_transitions() {
        let m = QueryManager::new();
        let id = m.submit(0);
        m.transition(&id, QueryState::Running).unwrap();
        m.transition(&id, QueryState::Succeeded).unwrap();
        assert_eq!(m.status(&id), Ok(QueryState::Succeeded));
    }

    #[test]
    fn cannot_leave_a_terminal_state() {
        let m = QueryManager::new();
        let id = m.submit(0);
        m.transition(&id, QueryState::Running).unwrap();
        m.transition(&id, QueryState::Succeeded).unwrap();
        assert_eq!(
            m.transition(&id, QueryState::Running),
            Err(QueryError::InvalidTransition {
                from: QueryState::Succeeded,
                to: QueryState::Running
            })
        );
    }

    #[test]
    fn cancel_works_from_queued_and_running() {
        let m = QueryManager::new();
        let a = m.submit(0);
        let b = m.submit(0);
        m.transition(&b, QueryState::Running).unwrap();
        assert_eq!(m.cancel(&a), Ok(()));
        assert_eq!(m.cancel(&b), Ok(()));
        assert_eq!(m.status(&a), Ok(QueryState::Cancelled));
        assert_eq!(m.status(&b), Ok(QueryState::Cancelled));
    }

    #[test]
    fn unknown_query_is_not_found() {
        let m = QueryManager::new();
        assert_eq!(m.status("q-404"), Err(QueryError::NotFound));
        assert_eq!(m.cancel("q-404"), Err(QueryError::NotFound));
    }
}
