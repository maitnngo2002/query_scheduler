//! Serialization of fragment plans containing the shuffle operators.
//!
//! [`ShuffleCodec`] plugs into `datafusion-proto` as a `PhysicalExtensionCodec`, so
//! a whole fragment (standard operators plus `ShuffleWriteExec` and
//! `ShuffleReadExec`) can be turned into bytes, sent to a worker, and rebuilt.
//!
//! Wire format of an extension node: one tag byte, then a protobuf message.
//! Children are serialized by DataFusion itself and handed back to `try_decode`.
//!
//! Two decisions worth knowing:
//!
//! * A partitioning holds physical expressions (the hash keys). Rather than
//!   serializing expressions by hand, it is wrapped in a throwaway
//!   `RepartitionExec` over an `EmptyExec` and serialized with DataFusion's own
//!   plan serialization, then unwrapped on decode. This works for any expression
//!   DataFusion can serialize.
//! * The decoding side supplies the `ShuffleStore` (the codec holds a handle).
//!   Network reads will replace that handle with a bucket fetcher.
//!
//! Known loss: a decoded `ShuffleReadExec` has no ordering metadata (the schema
//! and partitioning are kept). Execution does not depend on it.

use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use prost::Message;

use crate::shuffle::{ReadMode, ShuffleReadExec, ShuffleWriteExec, WriteMode};
use crate::store::{encode_batches, BucketSource, ShuffleStore};

const TAG_WRITE: u8 = 1;
const TAG_READ: u8 = 2;

#[derive(Clone, PartialEq, prost::Message)]
struct WriteNode {
    #[prost(string, tag = "1")]
    query_id: String,
    #[prost(uint64, tag = "2")]
    fragment: u64,
    /// False: everything goes to bucket 0. True: see `partitioning`.
    #[prost(bool, tag = "3")]
    partitioned: bool,
    /// A serialized throwaway `RepartitionExec` carrying the partitioning.
    #[prost(bytes = "vec", tag = "4")]
    partitioning: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ReadNode {
    #[prost(string, tag = "1")]
    query_id: String,
    #[prost(uint64, tag = "2")]
    input_fragment: u64,
    #[prost(uint64, tag = "3")]
    producer_tasks: u64,
    /// False: `ReadMode::Bucket`. True: `ReadMode::PerProducer`.
    #[prost(bool, tag = "4")]
    per_producer: bool,
    /// Arrow IPC bytes holding only the schema.
    #[prost(bytes = "vec", tag = "5")]
    schema: Vec<u8>,
    /// False: use `UnknownPartitioning(partition_count)`. True: see `partitioning`.
    #[prost(bool, tag = "6")]
    has_partitioning: bool,
    #[prost(uint64, tag = "7")]
    partition_count: u64,
    #[prost(bytes = "vec", tag = "8")]
    partitioning: Vec<u8>,
}

fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    encode_batches(schema, &[])
}

fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

fn encode_partitioning(schema: &SchemaRef, partitioning: &Partitioning) -> Result<Vec<u8>> {
    let throwaway: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        Arc::new(EmptyExec::new(Arc::clone(schema))),
        partitioning.clone(),
    )?);
    Ok(physical_plan_to_bytes_with_extension_codec(
        throwaway,
        &DefaultPhysicalExtensionCodec {},
    )?
    .to_vec())
}

fn decode_partitioning(bytes: &[u8], ctx: &TaskContext) -> Result<Partitioning> {
    let plan = physical_plan_from_bytes_with_extension_codec(
        bytes,
        ctx,
        &DefaultPhysicalExtensionCodec {},
    )?;
    let repartition = plan.downcast_ref::<RepartitionExec>().ok_or_else(|| {
        DataFusionError::Internal("encoded partitioning did not decode to a RepartitionExec".into())
    })?;
    Ok(repartition.partitioning().clone())
}

fn encode_err(e: prost::EncodeError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

fn decode_err(e: prost::DecodeError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Encodes and decodes `ShuffleWriteExec` and `ShuffleReadExec`.
///
/// Decoded writers write to `store`; decoded readers fetch buckets through `source`,
/// which is the same store when everything runs in one process, or a router that
/// fetches from other workers.
#[derive(Debug)]
pub struct ShuffleCodec {
    store: Arc<ShuffleStore>,
    source: Arc<dyn BucketSource>,
}

impl ShuffleCodec {
    /// Writers and readers both use `store` (single-process use).
    pub fn new(store: Arc<ShuffleStore>) -> Self {
        let source: Arc<dyn BucketSource> = Arc::clone(&store) as Arc<dyn BucketSource>;
        ShuffleCodec { store, source }
    }

    /// Writers write to `store`; readers fetch through `source` (for example a
    /// [`crate::net::RoutedSource`]).
    pub fn with_source(store: Arc<ShuffleStore>, source: Arc<dyn BucketSource>) -> Self {
        ShuffleCodec { store, source }
    }
}

impl PhysicalExtensionCodec for ShuffleCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (tag, body) = buf
            .split_first()
            .ok_or_else(|| DataFusionError::Internal("empty shuffle node payload".into()))?;
        match *tag {
            TAG_WRITE => {
                let msg = WriteNode::decode(body).map_err(decode_err)?;
                let input = match inputs {
                    [only] => Arc::clone(only),
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "ShuffleWriteExec needs exactly one input, got {}",
                            inputs.len()
                        )))
                    }
                };
                let mode = if msg.partitioned {
                    WriteMode::Partitioned(decode_partitioning(&msg.partitioning, ctx)?)
                } else {
                    WriteMode::Single
                };
                Ok(Arc::new(ShuffleWriteExec::new(
                    input,
                    mode,
                    msg.query_id,
                    msg.fragment as usize,
                    Arc::clone(&self.store),
                )))
            }
            TAG_READ => {
                let msg = ReadNode::decode(body).map_err(decode_err)?;
                if !inputs.is_empty() {
                    return Err(DataFusionError::Internal(
                        "ShuffleReadExec is a leaf and takes no inputs".into(),
                    ));
                }
                let schema = decode_schema(&msg.schema)?;
                let partitioning = if msg.has_partitioning {
                    decode_partitioning(&msg.partitioning, ctx)?
                } else {
                    Partitioning::UnknownPartitioning(msg.partition_count as usize)
                };
                let properties = Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema),
                    partitioning,
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ));
                let mode = if msg.per_producer { ReadMode::PerProducer } else { ReadMode::Bucket };
                Ok(Arc::new(ShuffleReadExec::new(
                    msg.query_id,
                    msg.input_fragment as usize,
                    msg.producer_tasks as usize,
                    mode,
                    properties,
                    Arc::clone(&self.source),
                )))
            }
            other => Err(DataFusionError::Internal(format!("unknown shuffle node tag {other}"))),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(write) = node.downcast_ref::<ShuffleWriteExec>() {
            let (partitioned, partitioning) = match write.mode() {
                WriteMode::Partitioned(p) => (true, encode_partitioning(&write.input().schema(), p)?),
                WriteMode::Single => (false, Vec::new()),
            };
            buf.push(TAG_WRITE);
            WriteNode {
                query_id: write.query_id().to_string(),
                fragment: write.fragment() as u64,
                partitioned,
                partitioning,
            }
            .encode(buf)
            .map_err(encode_err)?;
            return Ok(());
        }

        if let Some(read) = node.downcast_ref::<ShuffleReadExec>() {
            let partitioning = read.properties().output_partitioning().clone();
            let (has_partitioning, partition_count, encoded) = match &partitioning {
                Partitioning::Hash(_, _) | Partitioning::RoundRobinBatch(_) => {
                    (true, partitioning.partition_count() as u64, encode_partitioning(&read.schema(), &partitioning)?)
                }
                other => (false, other.partition_count() as u64, Vec::new()),
            };
            buf.push(TAG_READ);
            ReadNode {
                query_id: read.query_id().to_string(),
                input_fragment: read.input_fragment() as u64,
                producer_tasks: read.producer_tasks() as u64,
                per_producer: read.read_mode() == ReadMode::PerProducer,
                schema: encode_schema(&read.schema())?,
                has_partitioning,
                partition_count,
                partitioning: encoded,
            }
            .encode(buf)
            .map_err(encode_err)?;
            return Ok(());
        }

        Err(DataFusionError::NotImplemented(format!(
            "ShuffleCodec cannot encode {}",
            node.name()
        )))
    }
}

/// Serializes a fragment plan (standard operators plus shuffle operators).
pub fn encode_plan(plan: &Arc<dyn ExecutionPlan>, codec: &ShuffleCodec) -> Result<Vec<u8>> {
    Ok(physical_plan_to_bytes_with_extension_codec(Arc::clone(plan), codec)?.to_vec())
}

/// Rebuilds a fragment plan. The codec's store is what the rebuilt shuffle
/// operators read from and write to.
pub fn decode_plan(
    bytes: &[u8],
    context: &TaskContext,
    codec: &ShuffleCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    physical_plan_from_bytes_with_extension_codec(bytes, context, codec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    use crate::example;
    use crate::rewrite::{distribute, run_locally, DistributedPlan, FragmentPlan};

    fn render(batches: &[RecordBatch]) -> String {
        pretty_format_batches(batches).unwrap().to_string()
    }

    /// Distributes `sql`, serializes every fragment, decodes each one into a new plan,
    /// runs the decoded fragments, and requires the same output as plain DataFusion.
    async fn assert_serialized_matches(sql: &str) {
        let ctx = example::context().await.unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let expected = df.collect().await.unwrap();

        let store = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "q-codec", &store).unwrap();
        let codec = ShuffleCodec::new(Arc::clone(&store));
        let task_ctx = ctx.task_ctx();

        let mut decoded_fragments = Vec::new();
        for fragment in distributed.fragments {
            let bytes = encode_plan(&fragment.plan, &codec).unwrap();
            assert!(!bytes.is_empty());
            let decoded = decode_plan(&bytes, &task_ctx, &codec).unwrap();
            assert_eq!(decoded.name(), fragment.plan.name(), "root operator changed: {sql}");
            decoded_fragments.push(FragmentPlan { info: fragment.info, plan: decoded });
        }
        let decoded = DistributedPlan { fragments: decoded_fragments };

        let actual = tokio::time::timeout(Duration::from_secs(60), run_locally(&decoded, task_ctx))
            .await
            .unwrap_or_else(|_| panic!("decoded run timed out: {sql}"))
            .unwrap();
        assert_eq!(render(&actual), render(&expected), "results differ for: {sql}");
    }

    #[tokio::test]
    async fn example_query_survives_serialization() {
        assert_serialized_matches(example::QUERY).await;
    }

    #[tokio::test]
    async fn grouped_aggregate_survives_serialization() {
        assert_serialized_matches(
            "SELECT segment, COUNT(*) AS n, MIN(id) AS lo FROM customers GROUP BY segment ORDER BY segment",
        )
        .await;
    }

    #[tokio::test]
    async fn filtered_sorted_scan_survives_serialization() {
        assert_serialized_matches(
            "SELECT order_id, total FROM orders WHERE cust_id = 7 ORDER BY order_id",
        )
        .await;
    }

    #[tokio::test]
    async fn schema_round_trips_through_ipc() {
        let ctx = example::context().await.unwrap();
        let schema = ctx.table("customers").await.unwrap().schema().inner().clone();
        let decoded = decode_schema(&encode_schema(&schema).unwrap()).unwrap();
        assert_eq!(decoded, schema);
    }
}
