//! The two custom operators that replace exchanges when a plan is distributed.
//!
//! * [`ShuffleWriteExec`] wraps the root of a producer fragment. For task
//!   partition `i` it runs the fragment for input partition `i`, splits the
//!   output into buckets, stores them, and returns an empty stream.
//! * [`ShuffleReadExec`] is a leaf in the consumer fragment. It reads buckets
//!   back from the store.
//!
//! Hash buckets use DataFusion's own `BatchPartitioner`, so rows are placed
//! exactly as `RepartitionExec` would place them, and both sides of a
//! partitioned join agree on where each key goes.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PhysicalExpr, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};

use crate::store::{BucketSource, OutputKey, ShuffleStore};

/// How a producer task splits its output into buckets.
#[derive(Debug, Clone)]
pub enum WriteMode {
    /// Split rows with a DataFusion partitioning (hash or round-robin).
    Partitioned(Partitioning),
    /// Everything goes to bucket 0 (merge and coalesce exchanges).
    Single,
}

impl WriteMode {
    pub fn buckets(&self) -> usize {
        match self {
            WriteMode::Partitioned(p) => p.partition_count(),
            WriteMode::Single => 1,
        }
    }
}

#[derive(Debug)]
pub struct ShuffleWriteExec {
    input: Arc<dyn ExecutionPlan>,
    mode: WriteMode,
    query_id: String,
    fragment: usize,
    store: Arc<ShuffleStore>,
    properties: Arc<PlanProperties>,
}

impl ShuffleWriteExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        mode: WriteMode,
        query_id: String,
        fragment: usize,
        store: Arc<ShuffleStore>,
    ) -> Self {
        // One output partition per input partition (one per task); each yields no rows.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        ShuffleWriteExec { input, mode, query_id, fragment, store, properties }
    }
}

impl ShuffleWriteExec {
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn mode(&self) -> &WriteMode {
        &self.mode
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn fragment(&self) -> usize {
        self.fragment
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleWriteExec: fragment={}, buckets={}",
            self.fragment,
            self.mode.buckets()
        )
    }
}

impl ExecutionPlan for ShuffleWriteExec {
    fn name(&self) -> &str {
        "ShuffleWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // The hash keys are the only expressions this node evaluates during execution.
        if let WriteMode::Partitioned(Partitioning::Hash(exprs, _)) = &self.mode {
            for expr in exprs {
                if let TreeNodeRecursion::Stop = f(expr)? {
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ShuffleWriteExec takes exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(ShuffleWriteExec::new(
            children.swap_remove(0),
            self.mode.clone(),
            self.query_id.clone(),
            self.fragment,
            Arc::clone(&self.store),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let num_input_partitions = self.input.output_partitioning().partition_count();
        let buckets = self.mode.buckets();

        let metrics = ExecutionPlanMetricsSet::new();
        let timer = MetricBuilder::new(&metrics).subset_time("shuffle_write", partition);
        let mut partitioner = match &self.mode {
            WriteMode::Partitioned(p) => Some(BatchPartitioner::try_new(
                p.clone(),
                timer,
                partition,
                num_input_partitions,
            )?),
            WriteMode::Single => None,
        };

        let store = Arc::clone(&self.store);
        let query_id = self.query_id.clone();
        let fragment = self.fragment;
        let schema = self.schema();

        // All the work happens when the stream is first polled; it yields no rows.
        let work = async move {
            let mut out: Vec<Vec<RecordBatch>> = vec![Vec::new(); buckets];
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                match partitioner.as_mut() {
                    Some(p) => p.partition(batch, |bucket, b| {
                        out[bucket].push(b);
                        Ok(())
                    })?,
                    None => out[0].push(batch),
                }
            }
            for (bucket, batches) in out.into_iter().enumerate() {
                store.put(
                    OutputKey { query_id: query_id.clone(), fragment, task: partition, bucket },
                    batches,
                );
            }
            Ok::<(), DataFusionError>(())
        };

        let stream = futures::stream::once(work).filter_map(|result: Result<()>| async move {
            let item: Option<Result<RecordBatch>> = match result {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            };
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Which buckets an output partition of a [`ShuffleReadExec`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Output partition `j` reads bucket `j` from every producer task
    /// (hash and round-robin exchanges).
    Bucket,
    /// Output partition `p` reads bucket 0 of producer task `p`
    /// (merge and coalesce exchanges, where the consumer merges the streams).
    PerProducer,
}

#[derive(Debug)]
pub struct ShuffleReadExec {
    query_id: String,
    input_fragment: usize,
    producer_tasks: usize,
    mode: ReadMode,
    /// Copied from the node this reader replaces, so the operators above it see
    /// exactly the partitioning and ordering they saw before the cut.
    properties: Arc<PlanProperties>,
    /// Where buckets come from: the local store, or other workers over the network.
    source: Arc<dyn BucketSource>,
}

impl ShuffleReadExec {
    pub fn new(
        query_id: String,
        input_fragment: usize,
        producer_tasks: usize,
        mode: ReadMode,
        properties: Arc<PlanProperties>,
        source: Arc<dyn BucketSource>,
    ) -> Self {
        ShuffleReadExec { query_id, input_fragment, producer_tasks, mode, properties, source }
    }
}

impl ShuffleReadExec {
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn input_fragment(&self) -> usize {
        self.input_fragment
    }

    pub fn producer_tasks(&self) -> usize {
        self.producer_tasks
    }

    pub fn read_mode(&self) -> ReadMode {
        self.mode
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleReadExec: input_fragment={}, producers={}, mode={:?}",
            self.input_fragment, self.producer_tasks, self.mode
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // A reader owns no expressions.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ShuffleReadExec is a leaf and takes no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_partitions = self.properties.output_partitioning().partition_count();
        if partition >= output_partitions {
            return Err(DataFusionError::Internal(format!(
                "ShuffleReadExec partition {partition} out of range (has {output_partitions})"
            )));
        }

        let keys: Vec<OutputKey> = match self.mode {
            ReadMode::Bucket => (0..self.producer_tasks)
                .map(|task| OutputKey {
                    query_id: self.query_id.clone(),
                    fragment: self.input_fragment,
                    task,
                    bucket: partition,
                })
                .collect(),
            ReadMode::PerProducer => vec![OutputKey {
                query_id: self.query_id.clone(),
                fragment: self.input_fragment,
                task: partition,
                bucket: 0,
            }],
        };

        // Fetching may cross the network, so it happens when the stream is first polled.
        let source = Arc::clone(&self.source);
        let schema = self.schema();
        let fetch_all = async move {
            let mut batches: Vec<RecordBatch> = Vec::new();
            for key in keys {
                batches.extend(source.fetch(key).await?);
            }
            Ok::<_, DataFusionError>(futures::stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        };
        let stream = futures::stream::once(fetch_all).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
