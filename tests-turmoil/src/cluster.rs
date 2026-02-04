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
    /// Get the address for a node.
    pub fn addr(&self, id: NodeId) -> &str {
        &self.nodes.get(&id).expect("node not found").addr
    }

    /// Get the host name for a node.
    pub fn host_name(id: NodeId) -> String {
        format!("node-{}", id)
    }
}

/// Configuration for spawning a cluster.
pub struct ClusterConfig {
    /// Number of nodes.
    pub num_nodes: usize,
    /// Raft configuration.
    pub raft_config: Config,
    /// Seed for deterministic RNG.
    pub seed: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_nodes: 3,
            raft_config: Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 500,
                ..Default::default()
            },
            seed: 0,
        }
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

    /// Get state snapshots from all nodes.
    pub fn get_all_state_snapshots(&self) -> Vec<(NodeId, RaftStateSnapshot)> {
        self.rafts.iter().map(|(&id, raft)| (id, raft.state_snapshot())).collect()
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

/// Register a node's storage in the shared state BEFORE starting it.
pub fn register_node(
    node_id: NodeId,
    cluster_state: &Arc<std::sync::Mutex<ClusterState>>,
) {
    let mut state = cluster_state.lock().unwrap();
    let (log_store, state_machine) = new_store();
    state.log_stores.insert(node_id, log_store);
    state.state_machines.insert(node_id, state_machine);
}

/// Spawn a cluster of Raft nodes in the turmoil simulation.
///
/// Returns cluster info and a shared state for observing nodes.
pub fn spawn_cluster(
    sim: &mut Sim,
    config: ClusterConfig,
) -> (ClusterInfo, Arc<std::sync::Mutex<ClusterState>>) {
    let node_ids: Vec<NodeId> = (1..=config.num_nodes as u64).collect();
    let mut nodes = BTreeMap::new();

    // Build node addresses
    for &id in &node_ids {
        let addr = format!("{}:9000", ClusterInfo::host_name(id));
        nodes.insert(id, Node { addr });
    }

    let raft_config = Arc::new(config.raft_config);
    let cluster_state = Arc::new(std::sync::Mutex::new(ClusterState::new()));

    // Register all nodes first (to initialize persistent storage)
    for &node_id in &node_ids {
        register_node(node_id, &cluster_state);
    }

    // Spawn each node host
    for &node_id in &node_ids {
        let raft_config = raft_config.clone();
        let all_nodes = nodes.clone();
        let cluster_state = cluster_state.clone();
        let host_name = ClusterInfo::host_name(node_id);
        let seed = config.seed;

        sim.host(host_name, move || {
            let raft_config = raft_config.clone();
            let all_nodes = all_nodes.clone();
            let cluster_state = cluster_state.clone();
            let node_seed = seed.wrapping_add(node_id);

            async move {
                let rng = RefCell::new(SmallRng::seed_from_u64(node_seed));

                let res: Result<(), Box<dyn std::error::Error>> = DETERMINISTIC_RNG.scope(rng, async move {
                    // Start RPC server FIRST so other nodes can connect
                    let listener = TcpListener::bind("0.0.0.0:9000").await.expect("Failed to bind");
                    tracing::info!(node_id, "RPC server listening");

                    // Get existing storage from shared state
                    let (log_store, state_machine) = {
                        let state = cluster_state.lock().unwrap();
                        (
                            state.log_stores.get(&node_id).expect("node not registered").clone(),
                            state.state_machines.get(&node_id).expect("node not registered").clone(),
                        )
                    };

                    // Create Raft instance
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

                    // Register/Update this node's Raft instance for external observation
                    {
                        let mut state = cluster_state.lock().unwrap();
                        state.rafts.insert(node_id, raft.clone());
                    }

                    // Initialize cluster on node 1 if it's the first time starting
                    if node_id == 1 {
                        // Check if already initialized by looking at log
                        let is_initialized = {
                            use openraft::storage::RaftLogStorage;
                            let mut state = cluster_state.lock().unwrap();
                            let log_store = state.log_stores.get_mut(&node_id).unwrap();
                            log_store.get_log_state().await.unwrap().last_log_id.is_some()
                        };

                        if !is_initialized {
                            // Give other nodes time to start their listeners
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            tracing::info!("Initializing cluster on node {}", node_id);
                            raft.initialize(all_nodes.clone())
                                .await
                                .expect("Failed to initialize");
                        }
                    }

                    // Handle incoming connections
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
