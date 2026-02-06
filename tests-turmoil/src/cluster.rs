use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::async_runtime::WatchReceiver;
use openraft::Config;
use openraft_rt_tokio::DETERMINISTIC_RNG;
use rand_09::SeedableRng;
use rand_09::rngs::SmallRng;
use turmoil::net::TcpListener;
use turmoil::Sim;

use crate::network::handle_rpc;
use crate::network::TurmoilNetwork;
use crate::store::new_store;
use crate::store::LogStore;
use crate::store::StateMachine;
use crate::store::StateMachineData;
use crate::typ::*;

/// Information about a running cluster.
#[derive(Clone)]
pub struct ClusterInfo {
    /// Node IDs in the cluster.
    pub node_ids: Vec<NodeId>,
    /// Mapping from node ID to address.
    pub nodes: BTreeMap<NodeId, Node>,
}

impl ClusterInfo {
    /// Get the host name for a node.
    pub fn host_name(id: NodeId) -> String {
        format!("node-{}", id)
    }

    /// Get the address for a node.
    pub fn addr(&self, id: NodeId) -> &str {
        &self.nodes.get(&id).expect("node not found").addr
    }
}

/// Shared state for observing nodes from outside the simulation.
pub struct ClusterState {
    /// Raft instances for each node (keyed by node ID).
    pub rafts: BTreeMap<NodeId, Arc<Raft>>,
    /// Log stores for each node.
    pub log_stores: BTreeMap<NodeId, Arc<LogStore>>,
    /// State machines for each node.
    pub state_machines: BTreeMap<NodeId, Arc<StateMachine>>,
}

/// Combined snapshot of both Raft and State Machine state.
pub struct FullNodeSnapshot {
    pub raft: RaftStateSnapshot,
    pub sm: StateMachineData,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            rafts: BTreeMap::new(),
            log_stores: BTreeMap::new(),
            state_machines: BTreeMap::new(),
        }
    }

    /// Get metrics from all nodes.
    pub fn get_all_metrics(&self) -> Vec<(NodeId, RaftMetrics)> {
        self.rafts
            .iter()
            .map(|(&id, raft)| {
                let metrics = raft.metrics().borrow_watched().clone();
                (id, metrics)
            })
            .collect()
    }

    /// Get combined Raft and State Machine snapshots from all nodes.
    pub fn get_all_full_snapshots(&self) -> Vec<(NodeId, FullNodeSnapshot)> {
        self.rafts
            .iter()
            .map(|(&id, raft)| {
                let sm = self.state_machines.get(&id).expect("sm not found").get_data();
                let raft = raft.state_snapshot();
                (id, FullNodeSnapshot { raft, sm })
            })
            .collect()
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

/// Register a node's storage in the shared state BEFORE starting it.
pub fn register_node_storage(
    node_id: NodeId,
    cluster_state: &Arc<std::sync::Mutex<ClusterState>>,
) {
    let mut state = cluster_state.lock().unwrap();
    if !state.log_stores.contains_key(&node_id) {
        let (log_store, state_machine) = new_store();
        state.log_stores.insert(node_id, log_store);
        state.state_machines.insert(node_id, state_machine);
    }
}

/// Create a Turmoil host for a node.
pub fn spawn_host(
    sim: &mut Sim,
    node_id: NodeId,
    raft_config: Arc<openraft::Config>,
    cluster_state: Arc<std::sync::Mutex<ClusterState>>,
    seed: u64,
    all_nodes: BTreeMap<NodeId, Node>,
) {
    let host_name = ClusterInfo::host_name(node_id);
    sim.host(host_name, move || {
        let raft_config = raft_config.clone();
        let all_nodes = all_nodes.clone();
        let cluster_state = cluster_state.clone();
        let node_seed = seed.wrapping_add(node_id);

        async move {
            let rng = RefCell::new(SmallRng::seed_from_u64(node_seed));

            let res: Result<(), Box<dyn std::error::Error>> = DETERMINISTIC_RNG.scope(rng, async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await.expect("Failed to bind");
                tracing::info!(node_id, "RPC server listening");

                let (log_store, state_machine) = {
                    let state = cluster_state.lock().unwrap();
                    (
                        state.log_stores.get(&node_id).expect("node not registered").clone(),
                        state.state_machines.get(&node_id).expect("node not registered").clone(),
                    )
                };

                let raft = openraft::Raft::new(
                    node_id,
                    raft_config,
                    TurmoilNetwork,
                    log_store,
                    state_machine,
                )
                .await
                .expect("Failed to create Raft");

                let raft = Arc::new(raft);

                {
                    let mut state = cluster_state.lock().unwrap();
                    state.rafts.insert(node_id, raft.clone());
                }

                // Node 1 initializes if needed
                if node_id == 1 {
                    use openraft::storage::RaftLogStorage;
                    let is_initialized = {
                        let mut state = cluster_state.lock().unwrap();
                        let log_store = state.log_stores.get_mut(&node_id).unwrap();
                        log_store.get_log_state().await.unwrap().last_log_id.is_some()
                    };

                    if !is_initialized {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        tracing::info!("Initializing cluster on node {}", node_id);
                        raft.initialize(all_nodes.clone())
                            .await
                            .expect("Failed to initialize");
                    }
                }

                loop {
                    match listener.accept().await {
                        Ok((stream, _addr)) => {
                            let raft_clone = raft.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_rpc(raft_clone, stream).await {
                                    tracing::warn!("RPC handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Accept error: {}", e);
                        }
                    }
                }
            }).await;
            res
        }
    });
}

/// Configuration for spawning a cluster.
pub struct ClusterConfig {
    pub num_nodes: usize,
    pub raft_config: Config,
    pub seed: u64,
}

/// Spawn a cluster of Raft nodes in the turmoil simulation.
pub fn spawn_cluster(
    sim: &mut Sim,
    config: ClusterConfig,
) -> (ClusterInfo, Arc<std::sync::Mutex<ClusterState>>) {
    let node_ids: Vec<NodeId> = (1..=config.num_nodes as u64).collect();
    let mut nodes = BTreeMap::new();

    for &id in &node_ids {
        nodes.insert(id, Node { addr: format!("{}:9000", ClusterInfo::host_name(id)) });
    }

    let raft_config = Arc::new(config.raft_config);
    let cluster_state = Arc::new(std::sync::Mutex::new(ClusterState::new()));

    for &node_id in &node_ids {
        register_node_storage(node_id, &cluster_state);
        spawn_host(sim, node_id, raft_config.clone(), cluster_state.clone(), config.seed, nodes.clone());
    }

    (ClusterInfo { node_ids, nodes }, cluster_state)
}

/// Restart a node by bouncing it.
pub fn restart_node(
    sim: &mut Sim,
    node_id: NodeId,
) {
    let host_name = ClusterInfo::host_name(node_id);
    tracing::info!("RESTART: bouncing {}", host_name);
    sim.bounce(host_name);
}
