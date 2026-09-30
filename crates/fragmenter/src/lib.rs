//! Plan fragmenter prototype.
//!
//! Splits a physical plan tree into fragments at exchange boundaries.
//! Every `Exchange` node becomes a cut: the subtree below it turns into a
//! producer fragment (wrapped in `ShuffleWrite`), and the exchange is replaced
//! in the consumer by a `RemoteRead` pointing at that fragment.
//!
//! This prototype uses a small, self-contained plan model so the cut logic can
//! be developed and tested in isolation. A later phase adds an adapter from
//! DataFusion's `ExecutionPlan`.
//!
//! Invariant: fragment ids are assigned in post-order, and `id == index` in
//! `FragmentedPlan::fragments`. A fragment's dependencies therefore always have
//! smaller ids, and the root fragment is always last.

use std::collections::HashSet;
use std::fmt;

pub type FragmentId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeKind {
    Hash(Vec<String>),
    RoundRobin,
    Broadcast,
    Coalesce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggMode {
    Partial,
    Final,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanNode {
    Scan { table: String },
    Filter { predicate: String, input: Box<PlanNode> },
    Project { columns: Vec<String>, input: Box<PlanNode> },
    Aggregate { mode: AggMode, group_by: Vec<String>, input: Box<PlanNode> },
    Join { on: String, left: Box<PlanNode>, right: Box<PlanNode> },
    Sort { keys: Vec<String>, input: Box<PlanNode> },
    /// Data movement boundary in the input plan.
    Exchange { kind: ExchangeKind, input: Box<PlanNode> },
    /// Produced by the fragmenter: read the output of another fragment.
    RemoteRead { fragment_id: FragmentId },
    /// Produced by the fragmenter: root of a producer fragment.
    ShuffleWrite { kind: ExchangeKind, input: Box<PlanNode> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    pub id: FragmentId,
    pub root: PlanNode,
    pub dependencies: Vec<FragmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentedPlan {
    pub fragments: Vec<Fragment>,
    pub root: FragmentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentError {
    /// The input already contains `RemoteRead` or `ShuffleWrite` nodes.
    AlreadyFragmented,
}

impl fmt::Display for FragmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FragmentError::AlreadyFragmented => {
                write!(f, "plan already contains RemoteRead/ShuffleWrite nodes")
            }
        }
    }
}

impl std::error::Error for FragmentError {}

/// Split `plan` into fragments at every `Exchange`.
pub fn fragment(plan: PlanNode) -> Result<FragmentedPlan, FragmentError> {
    let mut builder = Builder { fragments: Vec::new() };
    let mut deps = Vec::new();
    let root_node = builder.visit(plan, &mut deps)?;
    let root_id = builder.fragments.len() as FragmentId;
    builder.fragments.push(Fragment { id: root_id, root: root_node, dependencies: deps });
    Ok(FragmentedPlan { fragments: builder.fragments, root: root_id })
}

/// Fragments that are not yet complete and whose dependencies are all complete.
pub fn ready_fragments(plan: &FragmentedPlan, completed: &HashSet<FragmentId>) -> Vec<FragmentId> {
    plan.fragments
        .iter()
        .filter(|f| !completed.contains(&f.id))
        .filter(|f| f.dependencies.iter().all(|d| completed.contains(d)))
        .map(|f| f.id)
        .collect()
}

struct Builder {
    fragments: Vec<Fragment>,
}

impl Builder {
    /// Rewrites `node`, emitting producer fragments as a side effect.
    /// `deps` collects the fragments the rewritten node reads from directly.
    fn visit(&mut self, node: PlanNode, deps: &mut Vec<FragmentId>) -> Result<PlanNode, FragmentError> {
        Ok(match node {
            PlanNode::Scan { table } => PlanNode::Scan { table },
            PlanNode::Filter { predicate, input } => {
                PlanNode::Filter { predicate, input: Box::new(self.visit(*input, deps)?) }
            }
            PlanNode::Project { columns, input } => {
                PlanNode::Project { columns, input: Box::new(self.visit(*input, deps)?) }
            }
            PlanNode::Aggregate { mode, group_by, input } => {
                PlanNode::Aggregate { mode, group_by, input: Box::new(self.visit(*input, deps)?) }
            }
            PlanNode::Sort { keys, input } => {
                PlanNode::Sort { keys, input: Box::new(self.visit(*input, deps)?) }
            }
            PlanNode::Join { on, left, right } => {
                let left = self.visit(*left, deps)?;
                let right = self.visit(*right, deps)?;
                PlanNode::Join { on, left: Box::new(left), right: Box::new(right) }
            }
            PlanNode::Exchange { kind, input } => {
                let mut child_deps = Vec::new();
                let child = self.visit(*input, &mut child_deps)?;
                let id = self.fragments.len() as FragmentId;
                self.fragments.push(Fragment {
                    id,
                    root: PlanNode::ShuffleWrite { kind, input: Box::new(child) },
                    dependencies: child_deps,
                });
                deps.push(id);
                PlanNode::RemoteRead { fragment_id: id }
            }
            PlanNode::RemoteRead { .. } | PlanNode::ShuffleWrite { .. } => {
                return Err(FragmentError::AlreadyFragmented);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(t: &str) -> PlanNode {
        PlanNode::Scan { table: t.to_string() }
    }
    fn filter(input: PlanNode) -> PlanNode {
        PlanNode::Filter { predicate: "x > 1".to_string(), input: Box::new(input) }
    }
    fn agg(mode: AggMode, input: PlanNode) -> PlanNode {
        PlanNode::Aggregate { mode, group_by: vec!["k".to_string()], input: Box::new(input) }
    }
    fn exchange(kind: ExchangeKind, input: PlanNode) -> PlanNode {
        PlanNode::Exchange { kind, input: Box::new(input) }
    }
    fn hash() -> ExchangeKind {
        ExchangeKind::Hash(vec!["k".to_string()])
    }
    fn remote(id: FragmentId) -> PlanNode {
        PlanNode::RemoteRead { fragment_id: id }
    }
    fn shuffle(kind: ExchangeKind, input: PlanNode) -> PlanNode {
        PlanNode::ShuffleWrite { kind, input: Box::new(input) }
    }
    fn join_of_two_scans() -> PlanNode {
        PlanNode::Join {
            on: "a.k = b.k".to_string(),
            left: Box::new(exchange(hash(), scan("a"))),
            right: Box::new(exchange(hash(), scan("b"))),
        }
    }

    #[test]
    fn no_exchange_gives_single_fragment() {
        let plan = filter(scan("t"));
        let out = fragment(plan.clone()).unwrap();
        assert_eq!(out.fragments.len(), 1);
        assert_eq!(out.root, 0);
        assert_eq!(out.fragments[0].root, plan);
        assert!(out.fragments[0].dependencies.is_empty());
    }

    #[test]
    fn single_exchange_splits_in_two() {
        let plan = agg(AggMode::Final, exchange(hash(), agg(AggMode::Partial, filter(scan("t")))));
        let out = fragment(plan).unwrap();

        assert_eq!(out.fragments.len(), 2);
        assert_eq!(out.root, 1);

        let producer = &out.fragments[0];
        assert_eq!(producer.id, 0);
        assert_eq!(producer.root, shuffle(hash(), agg(AggMode::Partial, filter(scan("t")))));
        assert!(producer.dependencies.is_empty());

        let consumer = &out.fragments[1];
        assert_eq!(consumer.root, agg(AggMode::Final, remote(0)));
        assert_eq!(consumer.dependencies, vec![0]);
    }

    #[test]
    fn join_with_two_exchanges_gives_three_fragments() {
        let out = fragment(join_of_two_scans()).unwrap();

        assert_eq!(out.fragments.len(), 3);
        assert_eq!(out.root, 2);
        assert_eq!(out.fragments[0].root, shuffle(hash(), scan("a")));
        assert_eq!(out.fragments[1].root, shuffle(hash(), scan("b")));
        assert_eq!(
            out.fragments[2].root,
            PlanNode::Join {
                on: "a.k = b.k".to_string(),
                left: Box::new(remote(0)),
                right: Box::new(remote(1)),
            }
        );
        assert_eq!(out.fragments[2].dependencies, vec![0, 1]);
    }

    #[test]
    fn chained_exchanges_form_a_chain() {
        let plan = PlanNode::Sort {
            keys: vec!["k".to_string()],
            input: Box::new(exchange(
                ExchangeKind::Coalesce,
                agg(AggMode::Final, exchange(hash(), scan("t"))),
            )),
        };
        let out = fragment(plan).unwrap();

        assert_eq!(out.fragments.len(), 3);
        assert_eq!(out.fragments[0].root, shuffle(hash(), scan("t")));
        assert!(out.fragments[0].dependencies.is_empty());
        assert_eq!(
            out.fragments[1].root,
            shuffle(ExchangeKind::Coalesce, agg(AggMode::Final, remote(0)))
        );
        assert_eq!(out.fragments[1].dependencies, vec![0]);
        assert_eq!(
            out.fragments[2].root,
            PlanNode::Sort { keys: vec!["k".to_string()], input: Box::new(remote(1)) }
        );
        assert_eq!(out.fragments[2].dependencies, vec![1]);
    }

    #[test]
    fn dependencies_always_have_smaller_ids_and_root_is_last() {
        let out = fragment(join_of_two_scans()).unwrap();
        for f in &out.fragments {
            assert_eq!(out.fragments[f.id as usize].id, f.id);
            assert!(f.dependencies.iter().all(|d| *d < f.id));
        }
        assert_eq!(out.root as usize, out.fragments.len() - 1);
    }

    #[test]
    fn rejects_already_fragmented_plans() {
        assert_eq!(fragment(remote(0)), Err(FragmentError::AlreadyFragmented));
        assert_eq!(
            fragment(filter(shuffle(hash(), scan("t")))),
            Err(FragmentError::AlreadyFragmented)
        );
    }

    #[test]
    fn ready_fragments_follow_dependencies() {
        let out = fragment(join_of_two_scans()).unwrap();
        let mut done: HashSet<FragmentId> = HashSet::new();

        assert_eq!(ready_fragments(&out, &done), vec![0, 1]);
        done.insert(0);
        assert_eq!(ready_fragments(&out, &done), vec![1]);
        done.insert(1);
        assert_eq!(ready_fragments(&out, &done), vec![2]);
        done.insert(2);
        assert!(ready_fragments(&out, &done).is_empty());
    }
}
