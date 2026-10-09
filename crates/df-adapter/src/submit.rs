//! Client side of `SubmitQuery`: turns a [`DistributedPlan`] into the
//! `DistributedQuery` message the scheduler accepts.
//!
//! The message carries everything the scheduler needs to build its task graph
//! (task counts, output buckets, which inputs are partitioned) plus each
//! fragment's serialized plan, which the scheduler passes to workers untouched.
//! That keeps the scheduler free of DataFusion.

use std::sync::Arc;

use datafusion::error::Result;
use scheduler_proto::v1::{DistributedFragment, DistributedQuery, FragmentInput};

use crate::codec::{encode_plan, ShuffleCodec};
use crate::rewrite::DistributedPlan;
use crate::store::ShuffleStore;
use crate::Exchange;

/// Describes and serializes every fragment of `plan`.
///
/// The fragment plans embed whatever query id `plan` was built with; workers
/// replace it with the id the scheduler assigns, so a placeholder is fine.
pub fn distributed_query(plan: &DistributedPlan) -> Result<DistributedQuery> {
    // Encoding never touches the store; the codec just needs one to exist.
    let codec = ShuffleCodec::new(Arc::new(ShuffleStore::new()));
    let fragments = plan
        .fragments
        .iter()
        .map(|fragment| {
            let info = &fragment.info;
            let inputs = info
                .inputs
                .iter()
                .map(|&input| FragmentInput {
                    fragment_id: input as u32,
                    partitioned: matches!(
                        plan.fragments[input].info.output,
                        Some(Exchange::Hash(_)) | Some(Exchange::RoundRobin(_))
                    ),
                })
                .collect();
            Ok(DistributedFragment {
                id: info.id as u32,
                tasks: info.tasks as u32,
                output_partitions: info.output_buckets() as u32,
                inputs,
                plan: encode_plan(&fragment.plan, &codec)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DistributedQuery { fragments })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::decode_plan;
    use crate::example;
    use crate::rewrite::distribute;

    #[tokio::test]
    async fn example_query_is_described_like_the_cut() {
        let ctx = example::context().await.unwrap();
        let plan = ctx.sql(example::QUERY).await.unwrap().create_physical_plan().await.unwrap();
        let store = Arc::new(ShuffleStore::new());
        let distributed = distribute(&plan, "placeholder", &store).unwrap();

        let query = distributed_query(&distributed).unwrap();
        let f = &query.fragments;

        assert_eq!(f.iter().map(|x| x.id).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(f.iter().map(|x| x.tasks).collect::<Vec<_>>(), vec![1, 1, 4, 4, 4, 1]);
        // Hash, round-robin, hash, hash write 4 buckets; the merged fragment and the root write 1.
        assert_eq!(f.iter().map(|x| x.output_partitions).collect::<Vec<_>>(), vec![4, 4, 4, 4, 1, 1]);

        let input = |fragment_id, partitioned| FragmentInput { fragment_id, partitioned };
        assert!(f[0].inputs.is_empty() && f[1].inputs.is_empty());
        assert_eq!(f[2].inputs, vec![input(1, true)]);
        assert_eq!(f[3].inputs, vec![input(0, true), input(2, true)]);
        assert_eq!(f[4].inputs, vec![input(3, true)]);
        assert_eq!(f[5].inputs, vec![input(4, false)], "the root merges, it does not read buckets");

        // Every fragment's bytes decode back into a plan.
        let codec = ShuffleCodec::new(store);
        for fragment in f {
            decode_plan(&fragment.plan, &ctx.task_ctx(), &codec).unwrap();
        }
    }
}
