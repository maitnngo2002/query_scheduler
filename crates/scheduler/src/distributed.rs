//! Builds the task graph for a `DistributedQuery`: a real plan that the client
//! has already cut into fragments.
//!
//! The scheduler never decodes the plan. It trusts the description (task
//! counts, output buckets, which inputs are partitioned), checks that it is
//! consistent, and passes each fragment's plan bytes through to workers.

use fragmenter::tasks::{InputSpec, Task, TaskGraph, TaskId};
use scheduler_proto::v1::DistributedQuery;

/// Validates `query` and expands it into tasks. Returns the task graph and the
/// plan bytes of each fragment, indexed by fragment id.
pub fn task_graph(query: &DistributedQuery) -> Result<(TaskGraph, Vec<Vec<u8>>), String> {
    let fragments = &query.fragments;
    if fragments.is_empty() {
        return Err("distributed query has no fragments".to_string());
    }

    for (index, f) in fragments.iter().enumerate() {
        if f.id as usize != index {
            return Err(format!("fragment at position {index} has id {}", f.id));
        }
        if f.tasks == 0 {
            return Err(format!("fragment {index} has no tasks"));
        }
        if f.output_partitions == 0 {
            return Err(format!("fragment {index} has zero output partitions"));
        }
        if f.plan.is_empty() {
            return Err(format!("fragment {index} has an empty plan"));
        }
        for input in &f.inputs {
            // Post-order ids: inputs come first, which also rules out cycles.
            let Some(producer) = fragments.get(input.fragment_id as usize).filter(|_| input.fragment_id < f.id)
            else {
                return Err(format!("fragment {index} reads fragment {}, which does not precede it", input.fragment_id));
            };
            // Every bucket a producer writes must be read by exactly one consumer task.
            if input.partitioned && producer.output_partitions != f.tasks {
                return Err(format!(
                    "fragment {index} has {} tasks but reads fragment {}, which writes {} buckets",
                    f.tasks, producer.id, producer.output_partitions
                ));
            }
            if !input.partitioned && producer.output_partitions != 1 {
                return Err(format!(
                    "fragment {index} reads only bucket 0 of fragment {}, which writes {} buckets",
                    producer.id, producer.output_partitions
                ));
            }
        }
    }
    // FetchResults reads bucket 0 of each root task.
    let root = fragments.last().unwrap();
    if root.output_partitions != 1 {
        return Err(format!("root fragment {} must write one bucket, not {}", root.id, root.output_partitions));
    }

    let mut tasks = Vec::new();
    for f in fragments {
        for partition in 0..f.tasks {
            let inputs = f
                .inputs
                .iter()
                .map(|input| InputSpec {
                    fragment: input.fragment_id,
                    producer_tasks: fragments[input.fragment_id as usize].tasks,
                    bucket: if input.partitioned { partition } else { 0 },
                })
                .collect();
            tasks.push(Task {
                id: TaskId { fragment: f.id, partition },
                output_partitions: f.output_partitions,
                inputs,
            });
        }
    }
    let graph = TaskGraph { tasks, fragment_task_counts: fragments.iter().map(|f| f.tasks).collect() };
    let plans = fragments.iter().map(|f| f.plan.clone()).collect();
    Ok((graph, plans))
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_proto::v1::{DistributedFragment, FragmentInput};

    fn input(fragment_id: u32, partitioned: bool) -> FragmentInput {
        FragmentInput { fragment_id, partitioned }
    }

    fn fragment(id: u32, tasks: u32, output_partitions: u32, inputs: Vec<FragmentInput>) -> DistributedFragment {
        DistributedFragment { id, tasks, output_partitions, inputs, plan: vec![id as u8 + 1] }
    }

    /// The shape `df-adapter` produces for the example query: 6 fragments,
    /// tasks (1, 1, 4, 4, 4, 1).
    fn example() -> DistributedQuery {
        DistributedQuery {
            fragments: vec![
                fragment(0, 1, 4, vec![]),                                // customers scan -> Hash(4)
                fragment(1, 1, 4, vec![]),                                // orders scan -> RoundRobin(4)
                fragment(2, 4, 4, vec![input(1, true)]),                  // filter -> Hash(4)
                fragment(3, 4, 4, vec![input(0, true), input(2, true)]),  // join + partial agg -> Hash(4)
                fragment(4, 4, 1, vec![input(3, true)]),                  // final agg + sort -> Merge
                fragment(5, 1, 1, vec![input(4, false)]),                 // merge, to the client
            ],
        }
    }

    fn task(graph: &TaskGraph, fragment: u32, partition: u32) -> &Task {
        graph.tasks.iter().find(|t| t.id == TaskId { fragment, partition }).unwrap()
    }

    #[test]
    fn example_expands_into_fifteen_tasks() {
        let (graph, plans) = task_graph(&example()).unwrap();
        assert_eq!(graph.fragment_task_counts, vec![1, 1, 4, 4, 4, 1]);
        assert_eq!(graph.tasks.len(), 15);
        assert_eq!(plans, (1..=6u8).map(|b| vec![b]).collect::<Vec<_>>());
    }

    #[test]
    fn partitioned_inputs_read_the_tasks_own_bucket() {
        let (graph, _) = task_graph(&example()).unwrap();
        let t = task(&graph, 3, 2);
        assert_eq!(
            t.inputs,
            vec![
                InputSpec { fragment: 0, producer_tasks: 1, bucket: 2 },
                InputSpec { fragment: 2, producer_tasks: 4, bucket: 2 },
            ]
        );
        assert_eq!(t.output_partitions, 4);
    }

    #[test]
    fn merge_input_reads_bucket_zero_of_every_producer() {
        let (graph, _) = task_graph(&example()).unwrap();
        let root = task(&graph, 5, 0);
        assert_eq!(root.inputs, vec![InputSpec { fragment: 4, producer_tasks: 4, bucket: 0 }]);
        assert_eq!(root.output_partitions, 1);
    }

    fn rejected(query: DistributedQuery, needle: &str) {
        let err = task_graph(&query).unwrap_err();
        assert!(err.contains(needle), "expected {needle:?} in {err:?}");
    }

    #[test]
    fn inconsistent_descriptions_are_rejected() {
        rejected(DistributedQuery { fragments: vec![] }, "no fragments");

        let mut q = example();
        q.fragments[2].id = 7;
        rejected(q, "has id 7");

        let mut q = example();
        q.fragments[1].tasks = 0;
        rejected(q, "no tasks");

        let mut q = example();
        q.fragments[0].output_partitions = 0;
        rejected(q, "zero output partitions");

        let mut q = example();
        q.fragments[3].plan.clear();
        rejected(q, "empty plan");

        let mut q = example();
        q.fragments[2].inputs = vec![input(4, true)];
        rejected(q, "does not precede");

        let mut q = example();
        q.fragments[2].inputs = vec![input(2, true)];
        rejected(q, "does not precede");

        let mut q = example();
        q.fragments[2].tasks = 3;
        rejected(q, "writes 4 buckets");

        let mut q = example();
        q.fragments[5].inputs = vec![input(3, false)];
        rejected(q, "reads only bucket 0");

        let mut q = example();
        q.fragments[5].output_partitions = 2;
        rejected(q, "root fragment");
    }
}
