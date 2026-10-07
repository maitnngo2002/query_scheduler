//! Cuts a real DataFusion physical plan into fragments.
//!
//! This is the analysis half of the plan adapter: it reports what the
//! fragments, task counts, and exchanges are, without rewriting the plan.
//! Rewriting (shuffle write/read nodes) comes in the next slice.
//!
//! Cut rules, derived from the plan DataFusion 55.1 produces:
//!
//! * `RepartitionExec` (hash or round-robin) is an exchange. The node itself is
//!   removed; the subtree below it becomes a producer fragment that writes
//!   buckets, and the consumer reads bucket `j` in its task `j`.
//! * `SortPreservingMergeExec` and `CoalescePartitionsExec` stay in the
//!   consumer fragment. Their child becomes a producer fragment, and the
//!   consumer reads each producer task's output as a separate input partition.
//!
//! A task is one partition of a fragment: it runs the fragment's plan for a
//! single partition index. A fragment therefore has as many tasks as its root
//! operator has output partitions.
//!
//! Fragment ids are assigned in post-order, so a fragment's inputs always have
//! smaller ids and the root fragment is last. A fragment can have no operators
//! of its own if two exchanges are stacked directly; it is then a pure re-shuffle.

use std::fmt;
use std::sync::Arc;

use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};

pub mod example;

/// How a fragment's output reaches its consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exchange {
    /// Hash-partitioned into this many buckets.
    Hash(usize),
    /// Round-robin into this many buckets.
    RoundRobin(usize),
    /// Order-preserving merge of all producer tasks (`SortPreservingMergeExec`).
    Merge,
    /// Plain concatenation of all producer tasks (`CoalescePartitionsExec`).
    Coalesce,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Hash(n) => write!(f, "Hash({n})"),
            Exchange::RoundRobin(n) => write!(f, "RoundRobin({n})"),
            Exchange::Merge => write!(f, "Merge"),
            Exchange::Coalesce => write!(f, "Coalesce"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentInfo {
    pub id: usize,
    /// Operator names inside this fragment, in pre-order. Exchanges that were
    /// replaced by a reader are not listed.
    pub operators: Vec<String>,
    /// Fragments this one reads from.
    pub inputs: Vec<usize>,
    /// How this fragment's output is delivered; None for the root fragment.
    pub output: Option<Exchange>,
    /// Number of tasks (output partitions of the fragment's root operator).
    pub tasks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutPlan {
    /// In post-order; the root fragment is last.
    pub fragments: Vec<FragmentInfo>,
}

impl CutPlan {
    pub fn root(&self) -> usize {
        self.fragments.len() - 1
    }

    pub fn total_tasks(&self) -> usize {
        self.fragments.iter().map(|f| f.tasks).sum()
    }
}

impl fmt::Display for CutPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for frag in &self.fragments {
            let output = frag
                .output
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "client".to_string());
            writeln!(
                f,
                "F{}  tasks={}  ops=[{}]  reads={:?}  output={}",
                frag.id,
                frag.tasks,
                frag.operators.join(", "),
                frag.inputs,
                output
            )?;
        }
        write!(f, "total tasks: {}", self.total_tasks())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CutError {
    /// A `RepartitionExec` with a partitioning we do not know how to shuffle.
    UnsupportedPartitioning(String),
}

impl fmt::Display for CutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CutError::UnsupportedPartitioning(p) => write!(f, "unsupported repartitioning: {p}"),
        }
    }
}

impl std::error::Error for CutError {}

/// Cuts `plan` into fragments at every exchange.
pub fn cut(plan: &Arc<dyn ExecutionPlan>) -> Result<CutPlan, CutError> {
    let mut cutter = Cutter { fragments: Vec::new() };
    cutter.fragment_below(plan, None)?;
    Ok(CutPlan { fragments: cutter.fragments })
}

#[derive(Default)]
struct OpenFragment {
    operators: Vec<String>,
    inputs: Vec<usize>,
}

struct Cutter {
    fragments: Vec<FragmentInfo>,
}

impl Cutter {
    /// Builds the fragment rooted at `root` and returns its id. Producer
    /// fragments found below are added first, so ids are in post-order.
    fn fragment_below(
        &mut self,
        root: &Arc<dyn ExecutionPlan>,
        output: Option<Exchange>,
    ) -> Result<usize, CutError> {
        let mut open = OpenFragment::default();
        self.walk(root, &mut open)?;
        let id = self.fragments.len();
        self.fragments.push(FragmentInfo {
            id,
            operators: open.operators,
            inputs: open.inputs,
            output,
            tasks: root.output_partitioning().partition_count(),
        });
        Ok(id)
    }

    fn walk(&mut self, plan: &Arc<dyn ExecutionPlan>, open: &mut OpenFragment) -> Result<(), CutError> {
        // Hash / round-robin repartition: the node is replaced by a reader.
        if plan.is::<RepartitionExec>() {
            let exchange = match plan.output_partitioning() {
                Partitioning::Hash(_, n) => Exchange::Hash(*n),
                Partitioning::RoundRobinBatch(n) => Exchange::RoundRobin(*n),
                other => return Err(CutError::UnsupportedPartitioning(format!("{other:?}"))),
            };
            for child in plan.children() {
                let id = self.fragment_below(child, Some(exchange.clone()))?;
                open.inputs.push(id);
            }
            return Ok(());
        }

        open.operators.push(plan.name().to_string());

        // Merge / coalesce: the operator stays here, its input is cut.
        let merge_exchange = if plan.is::<SortPreservingMergeExec>() {
            Some(Exchange::Merge)
        } else if plan.is::<CoalescePartitionsExec>() {
            Some(Exchange::Coalesce)
        } else {
            None
        };

        for child in plan.children() {
            match &merge_exchange {
                Some(exchange) => {
                    let id = self.fragment_below(child, Some(exchange.clone()))?;
                    open.inputs.push(id);
                }
                None => self.walk(child, open)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn example_plan(sql: &str) -> Arc<dyn ExecutionPlan> {
        let ctx = example::context().await.unwrap();
        ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap()
    }

    fn ops(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn example_query_cuts_into_six_fragments_and_fifteen_tasks() {
        let plan = example_plan(example::QUERY).await;
        let cut = cut(&plan).unwrap();
        let f = &cut.fragments;

        assert_eq!(f.len(), 6, "{cut}");

        // F0: customers scan, hashed on the join key.
        assert_eq!(f[0].operators, ops(&["DataSourceExec"]));
        assert_eq!(f[0].output, Some(Exchange::Hash(4)));
        assert!(f[0].inputs.is_empty());

        // F1: orders scan, round-robin into 4 partitions.
        assert_eq!(f[1].operators, ops(&["DataSourceExec"]));
        assert_eq!(f[1].output, Some(Exchange::RoundRobin(4)));

        // F2: filter, hashed on the join key.
        assert_eq!(f[2].operators, ops(&["FilterExec"]));
        assert_eq!(f[2].inputs, vec![1]);
        assert_eq!(f[2].output, Some(Exchange::Hash(4)));

        // F3: partitioned hash join plus the partial aggregate.
        assert_eq!(f[3].operators, ops(&["AggregateExec", "HashJoinExec"]));
        assert_eq!(f[3].inputs, vec![0, 2]);
        assert_eq!(f[3].output, Some(Exchange::Hash(4)));

        // F4: final aggregate, projection, and per-partition sort, merged by the root.
        assert_eq!(f[4].operators, ops(&["ProjectionExec", "SortExec", "AggregateExec"]));
        assert_eq!(f[4].inputs, vec![3]);
        assert_eq!(f[4].output, Some(Exchange::Merge));

        // F5: the order-preserving merge, delivered to the client.
        assert_eq!(f[5].operators, ops(&["SortPreservingMergeExec"]));
        assert_eq!(f[5].inputs, vec![4]);
        assert_eq!(f[5].output, None);

        let tasks: Vec<usize> = f.iter().map(|x| x.tasks).collect();
        assert_eq!(tasks, vec![1, 1, 4, 4, 4, 1], "{cut}");
        assert_eq!(cut.total_tasks(), 15);
        assert_eq!(cut.root(), 5);
    }

    #[tokio::test]
    async fn fragments_satisfy_structural_invariants() {
        for sql in [
            example::QUERY,
            "SELECT segment, COUNT(*) AS n FROM customers GROUP BY segment",
            "SELECT * FROM orders WHERE cust_id = 7",
        ] {
            let plan = example_plan(sql).await;
            let cut = cut(&plan).unwrap();

            for (i, frag) in cut.fragments.iter().enumerate() {
                assert_eq!(frag.id, i, "ids follow position: {sql}");
                assert!(frag.inputs.iter().all(|d| *d < frag.id), "inputs precede consumers: {sql}");
                assert!(frag.tasks >= 1, "every fragment has a task: {sql}");
            }
            let root = &cut.fragments[cut.root()];
            assert_eq!(root.output, None, "only the root is delivered to the client: {sql}");
            assert!(
                cut.fragments[..cut.root()].iter().all(|f| f.output.is_some()),
                "every non-root fragment has an exchange: {sql}"
            );
        }
    }
}
