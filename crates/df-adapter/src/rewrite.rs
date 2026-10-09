//! Rewrites a real physical plan into fragment plans a worker can run.
//!
//! Every exchange found by [`crate::cut`] is replaced: the subtree below it
//! becomes a producer fragment ending in a [`ShuffleWriteExec`], and the
//! consumer reads it through a [`ShuffleReadExec`]. Fragment ids and order match
//! [`crate::cut`] exactly (post-order, root last).

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use futures::TryStreamExt;

use crate::shuffle::{ReadMode, ShuffleReadExec, ShuffleWriteExec, WriteMode};
use crate::store::ShuffleStore;
use crate::{cut, FragmentInfo};

/// One fragment: its description and a plan that runs one task per partition.
#[derive(Debug)]
pub struct FragmentPlan {
    pub info: FragmentInfo,
    pub plan: Arc<dyn ExecutionPlan>,
}

/// All fragments in post-order; the root fragment is last.
#[derive(Debug)]
pub struct DistributedPlan {
    pub fragments: Vec<FragmentPlan>,
}

/// Cuts `plan` at its exchanges and builds the runnable fragment plans.
pub fn distribute(
    plan: &Arc<dyn ExecutionPlan>,
    query_id: &str,
    store: &Arc<ShuffleStore>,
) -> Result<DistributedPlan> {
    let cut_plan = cut(plan).map_err(|e| DataFusionError::Plan(e.to_string()))?;

    let mut rewriter = Rewriter { query_id, store, plans: Vec::new() };
    let root = rewriter.build(plan)?;
    rewriter.plans.push(root);

    if rewriter.plans.len() != cut_plan.fragments.len() {
        return Err(DataFusionError::Internal(format!(
            "rewriter produced {} fragments but the cutter found {}",
            rewriter.plans.len(),
            cut_plan.fragments.len()
        )));
    }
    let fragments = cut_plan
        .fragments
        .into_iter()
        .zip(rewriter.plans)
        .map(|(info, plan)| FragmentPlan { info, plan })
        .collect();
    Ok(DistributedPlan { fragments })
}

struct Rewriter<'a> {
    query_id: &'a str,
    store: &'a Arc<ShuffleStore>,
    /// Finished producer fragments, in post-order.
    plans: Vec<Arc<dyn ExecutionPlan>>,
}

impl Rewriter<'_> {
    /// Rebuilds `plan` for the fragment currently being built, cutting at exchanges.
    fn build(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // Hash / round-robin repartition: replaced by a reader of a new producer fragment.
        if plan.is::<RepartitionExec>() {
            let partitioning = plan.output_partitioning().clone();
            let child = plan.children()[0];
            let producer_tasks = child.output_partitioning().partition_count();
            let child_plan = self.build(child)?;

            let fragment_id = self.plans.len();
            self.plans.push(Arc::new(ShuffleWriteExec::new(
                child_plan,
                WriteMode::Partitioned(partitioning),
                self.query_id.to_string(),
                fragment_id,
                Arc::clone(self.store),
            )));
            return Ok(Arc::new(ShuffleReadExec::new(
                self.query_id.to_string(),
                fragment_id,
                producer_tasks,
                ReadMode::Bucket,
                Arc::clone(plan.properties()),
                Arc::clone(self.store),
            )));
        }

        // Merge / coalesce: the operator stays; each input becomes a producer fragment.
        let is_merge = plan.is::<SortPreservingMergeExec>() || plan.is::<CoalescePartitionsExec>();
        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for child in plan.children() {
            if is_merge {
                let producer_tasks = child.output_partitioning().partition_count();
                let child_plan = self.build(child)?;

                let fragment_id = self.plans.len();
                self.plans.push(Arc::new(ShuffleWriteExec::new(
                    child_plan,
                    WriteMode::Single,
                    self.query_id.to_string(),
                    fragment_id,
                    Arc::clone(self.store),
                )));
                new_children.push(Arc::new(ShuffleReadExec::new(
                    self.query_id.to_string(),
                    fragment_id,
                    producer_tasks,
                    ReadMode::PerProducer,
                    Arc::clone(child.properties()),
                    Arc::clone(self.store),
                )));
            } else {
                new_children.push(self.build(child)?);
            }
        }

        if new_children.is_empty() {
            Ok(Arc::clone(plan))
        } else {
            Arc::clone(plan).replace_children(
                new_children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        }
    }
}

/// Runs every task of every fragment, one after another, in this process, and
/// returns the root fragment's output. Fragments run in post-order, so every
/// producer finishes before its consumers start.
///
/// This is the reference for what the scheduler and workers must reproduce
/// across machines, and the harness the tests use to check results.
pub async fn run_locally(
    distributed: &DistributedPlan,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let root = distributed.fragments.len().saturating_sub(1);
    let mut result = Vec::new();
    for (index, fragment) in distributed.fragments.iter().enumerate() {
        for task in 0..fragment.info.tasks {
            let stream = fragment.plan.execute(task, Arc::clone(&context))?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            if index == root {
                result.extend(batches);
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use datafusion::arrow::util::pretty::pretty_format_batches;

    use crate::example;

    fn render(batches: &[RecordBatch]) -> String {
        pretty_format_batches(batches).unwrap().to_string()
    }

    /// Plans `sql`, runs it both with plain DataFusion and as distributed fragments
    /// executed task by task, and requires identical output.
    async fn assert_distributed_matches(sql: &str) {
        let ctx = example::context().await.unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let expected = df.collect().await.unwrap();

        let store = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "q-test", &store).unwrap();

        // The timeout turns a hang (for example a join waiting on other partitions) into a failure.
        let actual = tokio::time::timeout(
            Duration::from_secs(60),
            run_locally(&distributed, ctx.task_ctx()),
        )
        .await
        .unwrap_or_else(|_| panic!("distributed run timed out: {sql}"))
        .unwrap();

        assert_eq!(render(&actual), render(&expected), "results differ for: {sql}");
        assert!(!store.is_empty(), "shuffle output should have been written");
    }

    #[tokio::test]
    async fn example_query_matches_single_node_results() {
        assert_distributed_matches(example::QUERY).await;
    }

    #[tokio::test]
    async fn grouped_aggregate_matches_single_node_results() {
        assert_distributed_matches(
            "SELECT segment, COUNT(*) AS n, MIN(id) AS lo FROM customers GROUP BY segment ORDER BY segment",
        )
        .await;
    }

    #[tokio::test]
    async fn filtered_sorted_scan_matches_single_node_results() {
        assert_distributed_matches(
            "SELECT order_id, total FROM orders WHERE cust_id = 7 ORDER BY order_id",
        )
        .await;
    }

    #[tokio::test]
    async fn fragment_infos_match_the_cutter() {
        let ctx = example::context().await.unwrap();
        let plan = ctx
            .sql(example::QUERY)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let store = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "q-test", &store).unwrap();
        let cut_plan = cut(&plan).unwrap();

        let infos: Vec<FragmentInfo> = distributed.fragments.iter().map(|f| f.info.clone()).collect();
        assert_eq!(infos, cut_plan.fragments);
        assert_eq!(distributed.fragments.len(), 6);
    }

    #[tokio::test]
    async fn reading_before_the_producer_ran_is_an_error() {
        let ctx = example::context().await.unwrap();
        let plan = ctx
            .sql(example::QUERY)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let store = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "q-test", &store).unwrap();

        // The root fragment reads F4's output, which nobody has produced. The error may
        // surface when the task is started or when its stream is first polled.
        let root = distributed.fragments.last().unwrap();
        let outcome: Result<()> = match root.plan.execute(0, ctx.task_ctx()) {
            Err(e) => Err(e),
            Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await.map(|_| ()),
        };
        assert!(outcome.is_err(), "expected a missing-shuffle-output error");
    }
}
