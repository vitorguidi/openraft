//! Raft invariant checking.
//!
//! This module provides tools to verify that Raft invariants hold during
//! and after simulation runs.

use std::collections::HashMap;
use std::collections::HashSet;

use crate::store::StateMachineData;
use crate::typ::*;

/// Result of invariant checking.
#[derive(Debug)]
pub struct InvariantCheckResult {
    pub passed: bool,
    pub violations: Vec<InvariantViolation>,
}

impl InvariantCheckResult {
    pub fn ok() -> Self {
        Self {
            passed: true,
            violations: vec![],
        }
    }

    pub fn with_violations(violations: Vec<InvariantViolation>) -> Self {
        Self {
            passed: violations.is_empty(),
            violations,
        }
    }
}

/// Types of invariant violations that can be detected.
#[derive(Debug, Clone)]
pub enum InvariantViolation {
    /// Multiple leaders elected in the same term.
    MultipleLeadersInTerm {
        term: u64,
        leaders: Vec<NodeId>,
    },

    /// Log entries at the same index have different terms.
    LogMismatch {
        index: u64,
        node_a: NodeId,
        node_b: NodeId,
        term_a: u64,
        term_b: u64,
    },

    /// Different nodes applied different entries at the same index.
    StateMachineDivergence {
        index: u64,
        nodes: Vec<NodeId>,
    },

    /// A committed entry is missing from a later leader's log.
    LeaderMissingCommitted {
        term: u64,
        leader: NodeId,
        missing_index: u64,
    },

    /// Vote was granted to multiple candidates in the same term.
    MultipleVotesInTerm {
        voter: NodeId,
        term: u64,
        candidates: Vec<NodeId>,
    },
}

impl std::fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultipleLeadersInTerm { term, leaders } => {
                write!(f, "Multiple leaders {:?} in term {}", leaders, term)
            }
            Self::LogMismatch {
                index,
                node_a,
                node_b,
                term_a,
                term_b,
            } => {
                write!(
                    f,
                    "Log mismatch at index {}: node {} has term {}, node {} has term {}",
                    index, node_a, term_a, node_b, term_b
                )
            }
            Self::StateMachineDivergence { index, nodes } => {
                write!(
                    f,
                    "State machine divergence at index {} across nodes {:?}",
                    index, nodes
                )
            }
            Self::LeaderMissingCommitted {
                term,
                leader,
                missing_index,
            } => {
                write!(
                    f,
                    "Leader {} in term {} missing committed entry at index {}",
                    leader, term, missing_index
                )
            }
            Self::MultipleVotesInTerm {
                voter,
                term,
                candidates,
            } => {
                write!(
                    f,
                    "Node {} voted for multiple candidates {:?} in term {}",
                    voter, candidates, term
                )
            }
        }
    }
}

/// Snapshot of a node's state for invariant checking.
#[derive(Debug, Clone)]
pub struct NodeStateSnapshot {
    pub node_id: NodeId,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub state_machine: StateMachineData,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Option<Request>,
}

/// Invariant checker that collects and verifies cluster state.
pub struct InvariantChecker {
    node_states: HashMap<NodeId, NodeStateSnapshot>,
    /// Track votes per term: term -> (voter -> voted_for)
    votes_per_term: HashMap<u64, HashMap<NodeId, NodeId>>,
    /// Track leaders per term
    leaders_per_term: HashMap<u64, HashSet<NodeId>>,
}

impl InvariantChecker {
    pub fn new() -> Self {
        Self {
            node_states: HashMap::new(),
            votes_per_term: HashMap::new(),
            leaders_per_term: HashMap::new(),
        }
    }

    /// Add a node's state snapshot.
    pub fn add_node_state(&mut self, state: NodeStateSnapshot) {
        // Track votes
        if let Some(voted_for) = state.voted_for {
            self.votes_per_term
                .entry(state.current_term)
                .or_default()
                .insert(state.node_id, voted_for);
        }

        self.node_states.insert(state.node_id, state);
    }

    /// Record that a node became leader in a term.
    pub fn record_leader(&mut self, term: u64, leader: NodeId) {
        self.leaders_per_term.entry(term).or_default().insert(leader);
    }

    /// Run all invariant checks.
    pub fn check_all(&self) -> InvariantCheckResult {
        let mut violations = vec![];

        violations.extend(self.check_election_safety());
        violations.extend(self.check_log_matching());
        violations.extend(self.check_state_machine_safety());
        violations.extend(self.check_vote_safety());

        InvariantCheckResult::with_violations(violations)
    }

    /// INVARIANT 1: Election Safety
    /// At most one leader can be elected in a given term.
    pub fn check_election_safety(&self) -> Vec<InvariantViolation> {
        let mut violations = vec![];

        for (term, leaders) in &self.leaders_per_term {
            if leaders.len() > 1 {
                violations.push(InvariantViolation::MultipleLeadersInTerm {
                    term: *term,
                    leaders: leaders.iter().copied().collect(),
                });
            }
        }

        violations
    }

    /// INVARIANT 2: Log Matching
    /// If two logs contain an entry with the same index and term,
    /// then the logs are identical in all preceding entries.
    ///
    /// Note: We only check up to the commit index of each node.
    pub fn check_log_matching(&self) -> Vec<InvariantViolation> {
        let mut violations = vec![];
        let node_ids: Vec<_> = self.node_states.keys().copied().collect();

        for i in 0..node_ids.len() {
            for j in (i + 1)..node_ids.len() {
                let node_a = node_ids[i];
                let node_b = node_ids[j];

                let state_a = &self.node_states[&node_a];
                let state_b = &self.node_states[&node_b];

                let log_a = &state_a.log;
                let log_b = &state_b.log;

                let commit_a = state_a.commit_index;
                let commit_b = state_b.commit_index;

                // Build index maps, only including entries up to commit_index
                let map_a: HashMap<u64, u64> = log_a.iter().filter(|e| e.index <= commit_a).map(|e| (e.index, e.term)).collect();
                let map_b: HashMap<u64, u64> = log_b.iter().filter(|e| e.index <= commit_b).map(|e| (e.index, e.term)).collect();

                // Check for matching entries
                for (index, term_a) in &map_a {
                    if let Some(term_b) = map_b.get(index) {
                        if term_a != term_b {
                            violations.push(InvariantViolation::LogMismatch {
                                index: *index,
                                node_a,
                                node_b,
                                term_a: *term_a,
                                term_b: *term_b,
                            });
                        }
                    }
                }
            }
        }

        violations
    }

    /// INVARIANT 3: State Machine Safety
    /// If a server has applied a log entry at a given index to its state machine,
    /// no other server will ever apply a different log entry for the same index.
    pub fn check_state_machine_safety(&self) -> Vec<InvariantViolation> {
        let violations = vec![];

        // Group nodes by their state machine data
        let mut key_values: HashMap<String, HashMap<String, HashSet<NodeId>>> = HashMap::new();

        for (node_id, state) in &self.node_states {
            for (key, value) in &state.state_machine.data {
                key_values
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(*node_id);
            }
        }

        // Check for divergence - if a key has different values on different nodes
        // that have both applied it, that's a potential violation
        // Note: This is a simplified check. A more thorough check would verify
        // the exact applied index.
        for (key, values) in &key_values {
            if values.len() > 1 {
                // Multiple different values for the same key
                // This could be legitimate if nodes are at different applied indices
                // For now, just log it as potential divergence
                tracing::debug!(
                    "Key {} has {} different values across nodes",
                    key,
                    values.len()
                );
            }
        }

        violations
    }

    /// INVARIANT: Vote Safety
    /// Each node can vote for at most one candidate in a given term.
    pub fn check_vote_safety(&self) -> Vec<InvariantViolation> {
        // This invariant should be enforced by the Raft implementation itself,
        // but we check it anyway as a sanity check.
        // With our current data collection, each node only stores its most recent vote,
        // so we can't easily detect if it voted multiple times in the same term.
        vec![]
    }
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

use crate::cluster::FullNodeSnapshot;

/// Check invariants based on collected state snapshots from nodes.
pub fn check_state_invariants(snapshots: &[(NodeId, FullNodeSnapshot)]) -> InvariantCheckResult {
    let mut violations = Vec::new();

    // Check: At most one leader per term
    let mut leaders_by_term: HashMap<u64, Vec<NodeId>> = HashMap::new();
    for (node_id, s) in snapshots {
        if s.raft.server_state == openraft::ServerState::Leader {
            leaders_by_term.entry(s.raft.vote.leader_id().term).or_default().push(*node_id);
        }
    }

    for (term, leaders) in &leaders_by_term {
        if leaders.len() > 1 {
            violations.push(InvariantViolation::MultipleLeadersInTerm {
                term: *term,
                leaders: leaders.clone(),
            });
        }
    }

    // Check: Log consistency and State Machine Safety
    for i in 0..snapshots.len() {
        for j in (i + 1)..snapshots.len() {
            let (id_a, s_a) = &snapshots[i];
            let (id_b, s_b) = &snapshots[j];

            // 1. Log consistency (only for committed entries)
            let last_a = s_a.raft.committed.map(|id: LogId| id.index()).unwrap_or(0);
            let last_b = s_b.raft.committed.map(|id: LogId| id.index()).unwrap_or(0);

            let first_a = s_a.raft.log_ids.purged().map(|id: &LogId| id.index()).unwrap_or(0);
            let first_b = s_b.raft.log_ids.purged().map(|id: &LogId| id.index()).unwrap_or(0);

            let start = std::cmp::max(first_a, first_b);
            let end = std::cmp::min(last_a, last_b);

            for idx in start..=end {
                let term_a = s_a.raft.log_ids.get(idx).map(|id: LogId| id.committed_leader_id().term);
                let term_b = s_b.raft.log_ids.get(idx).map(|id: LogId| id.committed_leader_id().term);

                if let (Some(ta), Some(tb)) = (term_a, term_b) {
                    if ta != tb {
                        violations.push(InvariantViolation::LogMismatch {
                            index: idx,
                            node_a: *id_a,
                            node_b: *id_b,
                            term_a: ta,
                            term_b: tb,
                        });
                    }
                }
            }

            // 2. State Machine Safety
            // If both nodes have applied up to the same index, their state machines must match exactly.
            if let (Some(applied_a), Some(applied_b)) = (s_a.sm.last_applied, s_b.sm.last_applied) {
                if applied_a == applied_b {
                    if s_a.sm.data != s_b.sm.data {
                        violations.push(InvariantViolation::StateMachineDivergence {
                            index: applied_a.index() as u64,
                            nodes: vec![*id_a, *id_b],
                        });
                    }
                }
            }
        }
    }

    InvariantCheckResult::with_violations(violations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_safety_violation() {
        let mut checker = InvariantChecker::new();
        checker.record_leader(1, 1);
        checker.record_leader(1, 2);

        let result = checker.check_election_safety();
        assert_eq!(result.len(), 1);
        assert!(matches!(
            &result[0],
            InvariantViolation::MultipleLeadersInTerm { term: 1, .. }
        ));
    }

    #[test]
    fn test_log_matching_violation() {
        let mut checker = InvariantChecker::new();

        checker.add_node_state(NodeStateSnapshot {
            node_id: 1,
            current_term: 1,
            voted_for: None,
            log: vec![
                LogEntry { index: 1, term: 1, data: None },
                LogEntry { index: 2, term: 1, data: None },
            ],
            commit_index: 2,
            last_applied: 2,
            state_machine: StateMachineData::default(),
        });

        checker.add_node_state(NodeStateSnapshot {
            node_id: 2,
            current_term: 1,
            voted_for: None,
            log: vec![
                LogEntry { index: 1, term: 1, data: None },
                LogEntry { index: 2, term: 2, data: None }, // Different term!
            ],
            commit_index: 2,
            last_applied: 2,
            state_machine: StateMachineData::default(),
        });

        let result = checker.check_log_matching();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], InvariantViolation::LogMismatch { index: 2, .. }));
    }
}
