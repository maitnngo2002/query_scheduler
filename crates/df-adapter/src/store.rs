//! In-memory store for shuffle output, plus Arrow IPC helpers.
//!
//! A producer task writes its output as buckets, keyed by
//! `(query, fragment, task, bucket)`. Consumers read buckets back. Today the
//! store is read in-process; the worker will serve the same buckets over
//! `FetchTaskOutput`, using the IPC helpers to encode them.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Mutex;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::future::BoxFuture;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutputKey {
    pub query_id: String,
    pub fragment: usize,
    /// The producer task (its partition index within the fragment).
    pub task: usize,
    pub bucket: usize,
}

#[derive(Debug, Default)]
pub struct ShuffleStore {
    outputs: Mutex<HashMap<OutputKey, Vec<RecordBatch>>>,
}

impl ShuffleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores one bucket. An empty `batches` is stored too, so readers can tell
    /// "empty bucket" apart from "producer never ran".
    pub fn put(&self, key: OutputKey, batches: Vec<RecordBatch>) {
        self.outputs.lock().unwrap().insert(key, batches);
    }

    /// Returns a bucket's batches, or None if it was never written.
    /// Cloning a `RecordBatch` copies only reference counts.
    pub fn get(&self, key: &OutputKey) -> Option<Vec<RecordBatch>> {
        self.outputs.lock().unwrap().get(key).cloned()
    }

    /// Removes every bucket of a query and returns how many were removed.
    pub fn remove_query(&self, query_id: &str) -> usize {
        let mut outputs = self.outputs.lock().unwrap();
        let before = outputs.len();
        outputs.retain(|k, _| k.query_id != query_id);
        before - outputs.len()
    }

    pub fn len(&self) -> usize {
        self.outputs.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Where a `ShuffleReadExec` gets a bucket from: this process's store, or another worker.
pub trait BucketSource: std::fmt::Debug + Send + Sync {
    /// Returns all batches of one bucket. A bucket that was never written is an error;
    /// an empty bucket is an empty list.
    fn fetch(&self, key: OutputKey) -> BoxFuture<'static, Result<Vec<RecordBatch>>>;
}

impl BucketSource for ShuffleStore {
    fn fetch(&self, key: OutputKey) -> BoxFuture<'static, Result<Vec<RecordBatch>>> {
        let result = self.get(&key).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "missing shuffle output {key:?}: the producer task has not run"
            ))
        });
        Box::pin(futures::future::ready(result))
    }
}

/// Encodes batches in the Arrow IPC stream format (what `FetchTaskOutput` will carry).
pub fn encode_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner()?)
}

pub fn decode_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("n", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ("s", Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef),
        ])
        .unwrap()
    }

    fn key(query: &str, fragment: usize, task: usize, bucket: usize) -> OutputKey {
        OutputKey { query_id: query.to_string(), fragment, task, bucket }
    }

    #[test]
    fn put_and_get_round_trip() {
        let store = ShuffleStore::new();
        store.put(key("q", 0, 1, 2), vec![batch()]);
        let got = store.get(&key("q", 0, 1, 2)).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].num_rows(), 3);
        assert!(store.get(&key("q", 0, 1, 3)).is_none());
    }

    #[test]
    fn empty_buckets_are_distinct_from_missing_ones() {
        let store = ShuffleStore::new();
        store.put(key("q", 0, 0, 0), vec![]);
        assert_eq!(store.get(&key("q", 0, 0, 0)), Some(vec![]));
        assert_eq!(store.get(&key("q", 0, 0, 1)), None);
    }

    #[test]
    fn remove_query_only_removes_that_query() {
        let store = ShuffleStore::new();
        store.put(key("a", 0, 0, 0), vec![batch()]);
        store.put(key("a", 0, 0, 1), vec![batch()]);
        store.put(key("b", 0, 0, 0), vec![batch()]);
        assert_eq!(store.remove_query("a"), 2);
        assert_eq!(store.len(), 1);
        assert!(store.get(&key("b", 0, 0, 0)).is_some());
    }

    #[test]
    fn store_acts_as_a_bucket_source() {
        let store = ShuffleStore::new();
        store.put(key("q", 0, 0, 0), vec![batch()]);
        store.put(key("q", 0, 0, 1), vec![]);

        let got = futures::executor::block_on(store.fetch(key("q", 0, 0, 0))).unwrap();
        assert_eq!(got.len(), 1);
        let empty = futures::executor::block_on(store.fetch(key("q", 0, 0, 1))).unwrap();
        assert!(empty.is_empty());
        let missing = futures::executor::block_on(store.fetch(key("q", 0, 0, 2)));
        assert!(missing.is_err());
    }

    #[test]
    fn ipc_round_trip_preserves_data() {
        let b = batch();
        let bytes = encode_batches(&b.schema(), &[b.clone(), b.clone()]).unwrap();
        let decoded = decode_batches(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], b);
        assert_eq!(decoded[1], b);
    }

    #[test]
    fn ipc_round_trip_of_zero_batches() {
        let b = batch();
        let bytes = encode_batches(&b.schema(), &[]).unwrap();
        assert!(decode_batches(&bytes).unwrap().is_empty());
    }
}
