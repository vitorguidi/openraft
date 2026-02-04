//! Tick-based fuzzer for OpenRaft turmoil tests.
//!
//! Pattern:
//!   loop {
//!       sim.step()          // advance simulation one tick
//!       check_invariants()  // check from outside the simulation
//!   }
//!
//! Run with: cargo run --bin fuzz
//! Or with options: cargo run --bin fuzz -- --seed 12345 --fail-rate 0.1

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::ReadPolicy;
use rand_09::rngs::StdRng;
use rand_09::{Rng, SeedableRng};
use rand::rngs::SmallRng as SmallRng08;
use rand::SeedableRng as SeedableRng08;

use tests_turmoil::cluster::{spawn_host, register_node_storage, ClusterConfig, ClusterInfo};
use tests_turmoil::invariants::check_state_invariants;
use tests_turmoil::store::StateMachineData;
use tests_turmoil::typ::*;

/// Fuzzer configuration
struct FuzzConfig {
    /// RNG seed for reproducibility (None = random)
    seed: Option<u64>,
    /// Number of iterations to run (0 = forever)
    iterations: u64,
    /// Maximum number of steps per iteration
    max_steps: u64,
}

impl Default for FuzzConfig {
    fn default() -> Self {
        Self {
            seed: None, // Random
            iterations: 0, // Forever
            max_steps: 100_000,
        }
    }
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("openraft=info".parse().unwrap())
                .add_directive("tests_turmoil=info".parse().unwrap()),
        )
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .init();

    // Parse args
    let args: Vec<String> = std::env::args().collect();
    let mut config = FuzzConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" | "-s" => {
                i += 1;
                config.seed = Some(args[i].parse().expect("Invalid seed"));
            }
            "--iterations" | "-i" => {
                i += 1;
                config.iterations = args[i].parse().expect("Invalid iterations");
            }
            "--steps" => {
                i += 1;
                config.max_steps = args[i].parse().expect("Invalid steps");
            }
            "--help" | "-h" => {
                println!("OpenRaft Turmoil Fuzzer (state-space explorer)");
                println!();
                println!("Usage: fuzz [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -s, --seed <SEED>          RNG seed for reproducibility [default: random]");
                println!("  -i, --iterations <N>       Number of iterations (0 = forever) [default: 0]");
                println!("      --steps <N>            Steps per iteration [default: 100000]");
                println!("  -h, --help                 Show this help");
                return;
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Generate starting seed if not specified
    let start_seed = config.seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });

    println!("=== OpenRaft Turmoil Fuzzer (state-space explorer) ===");
    println!("Start seed: {}", start_seed);
    println!("Iterations: {} (0 = forever)", config.iterations);
    println!("Steps/iter: {}", config.max_steps);
    println!("======================================================");
    println!();

    // Set up Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    ctrlc::set_handler(move || {
        println!("\n\nInterrupted!");
        running_clone.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Run iterations
    let mut iteration = 0u64;
    let mut total_steps = 0u64;
    let mut total_checks = 0u64;
    let mut global_unique_states = HashSet::new();

    loop {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        // Check iteration limit (0 = forever)
        if config.iterations > 0 && iteration >= config.iterations {
            break;
        }

        let seed = start_seed.wrapping_add(iteration);
        println!("--- Iteration {} (seed: {}) ---", iteration + 1, seed);

        let result = run_fuzz_test(&config, seed, running.clone());
        total_steps += result.steps_completed;
        total_checks += result.invariant_checks;
        
        for state in result.unique_states {
            global_unique_states.insert(state);
        }

        print_final_sm_data(&result.cluster_state);

        if !result.violations.is_empty() {
            // Print failure results
            println!();
            println!("=== FAILED at iteration {} ===", iteration + 1);
            println!("Failing seed: {}", seed);
            println!("Steps completed: {}", result.steps_completed);
            println!("Unique states explored: {}", global_unique_states.len());
            println!("Total invariant checks: {}", total_checks);
            println!();
            println!("Violations:");
            for v in &result.violations {
                println!("  - {}", v);
            }
            println!();
            println!("Reproduce with: cargo run --bin fuzz -- --seed {}", seed);
            std::process::exit(1);
        }

        iteration += 1;
        println!("Unique states discovered so far: {}", global_unique_states.len());
    }

    // Print success results
    println!();
    println!("=== Final Results ===");
    println!("Iterations completed: {}", iteration);
    println!("Total steps: {}", total_steps);
    println!("Total invariant checks: {}", total_checks);
    println!("Unique cluster states explored: {}", global_unique_states.len());
    println!("Status: PASSED");
}

struct FuzzResult {
    steps_completed: u64,
    invariant_checks: u64,
    violations: Vec<String>,
    unique_states: HashSet<u64>,
    cluster_state: Arc<Mutex<tests_turmoil::cluster::ClusterState>>,
}

fn run_fuzz_test(config: &FuzzConfig, seed: u64, running: Arc<AtomicBool>) -> FuzzResult {
    // 1. Initialize RNG from seed to derive ALL parameters
    let mut param_rng = StdRng::seed_from_u64(seed);
    
    let num_initial_nodes = param_rng.gen_range(3..=5);
    let max_potential_nodes = 10u64;
    let fail_rate = param_rng.gen_range(0.0..0.08); 
    let heartbeat_interval = param_rng.gen_range(50..150);
    let election_timeout_min = heartbeat_interval * param_rng.gen_range(2..4);
    let election_timeout_max = election_timeout_min + param_rng.gen_range(100..500);
    
    // Percentages for chaos events
    let restart_chance = param_rng.gen_range(0.01..0.05);
    let chaos_interval = param_rng.gen_range(2000..5000);
    let membership_interval = param_rng.gen_range(10000..25000);

    println!("Simulation Params: nodes={}, fail_rate={:.1}%, hb={}ms, elect={}..{}ms", 
        num_initial_nodes, fail_rate * 100.0, heartbeat_interval, election_timeout_min, election_timeout_max);

    // 2. Initialize simulation with deterministic RNG
    let sim_rng = Box::new(SmallRng08::seed_from_u64(seed));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3600))
        .fail_rate(fail_rate)
        .enable_random_order()
        .tcp_capacity(65536)
        .build_with_rng(sim_rng);

    let raft_config = Arc::new(openraft::Config {
        heartbeat_interval,
        election_timeout_min,
        election_timeout_max,
        ..Default::default()
    });

    let cluster_state = Arc::new(Mutex::new(tests_turmoil::cluster::ClusterState::new()));
    let next_membership = Arc::new(Mutex::new(None::<HashSet<NodeId>>));

    // Pre-register ALL hosts
    let mut all_possible_nodes = BTreeMap::new();
    for id in 1..=max_potential_nodes {
        all_possible_nodes.insert(id, Node { addr: format!("{}:9000", ClusterInfo::host_name(id)) });
    }

    for id in 1..=max_potential_nodes {
        register_node_storage(id, &cluster_state);
        spawn_host(&mut sim, id, raft_config.clone(), cluster_state.clone(), seed, all_possible_nodes.clone());
    }

    // Add chaos agent
    {
        let chaos_seed = seed.wrapping_add(1000);
        sim.client("chaos-agent", async move {
            let mut rng = StdRng::seed_from_u64(chaos_seed);
            loop {
                let delay = rng.gen_range(1000..5000);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let chaos_type = rng.gen_range(0..5);
                match chaos_type {
                    0 => {
                        let victim = rng.gen_range(1..=max_potential_nodes);
                        let victim_name = format!("node-{}", victim);
                        for i in 1..=max_potential_nodes {
                            if i != victim { turmoil::partition(victim_name.clone(), format!("node-{}", i)); }
                        }
                    }
                    1 => {
                        for i in 1..=max_potential_nodes {
                            for j in (i + 1)..=max_potential_nodes { turmoil::repair(format!("node-{}", i), format!("node-{}", j)); }
                        }
                    }
                    2 => {
                        let a = rng.gen_range(1..=max_potential_nodes);
                        let mut b = rng.gen_range(1..=max_potential_nodes);
                        while b == a { b = rng.gen_range(1..=max_potential_nodes); }
                        turmoil::hold(format!("node-{}", a), format!("node-{}", b));
                    }
                    3 => {
                        for i in 1..=max_potential_nodes {
                            for j in 1..=max_potential_nodes { 
                                if i != j { turmoil::release(format!("node-{}", i), format!("node-{}", j)); }
                            }
                        }
                    }
                    _ => {}
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        });
    }

    // Add membership-agent
    {
        let cluster_state_clone = cluster_state.clone();
        let next_membership_clone = next_membership.clone();
        sim.client("membership-agent", async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let target_set = {
                    let mut guard = next_membership_clone.lock().unwrap();
                    guard.take()
                };
                if let Some(new_set) = target_set {
                    let leader = {
                        let state = cluster_state_clone.lock().unwrap();
                        state.rafts.iter()
                            .find(|(_, raft)| {
                                use openraft::async_runtime::WatchReceiver;
                                raft.metrics().borrow_watched().state.is_leader()
                            })
                            .map(|(_, raft)| raft.clone())
                    };
                    if let Some(raft) = leader {
                        println!("MEMBERSHIP-AGENT: executing change to {:?}", new_set);
                        let _ = raft.change_membership(new_set, false).await;
                    } else {
                        // Re-queue on failure
                        let mut guard = next_membership_clone.lock().unwrap();
                        if guard.is_none() { *guard = Some(new_set); }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        });
    }

    // Add workload client
    {
        let cluster_state_clone = cluster_state.clone();
        let workload_seed = seed.wrapping_add(2000);
        sim.client("workload", async move {
            let mut rng = StdRng::seed_from_u64(workload_seed);
            let mut op_count = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(rng.gen_range(10..50))).await;
                let leader = {
                    let state = cluster_state_clone.lock().unwrap();
                    state.rafts.iter().find(|(_, raft)| {
                        use openraft::async_runtime::WatchReceiver;
                        raft.metrics().borrow_watched().state.is_leader()
                    }).map(|(_, raft)| raft.clone())
                };

                if let Some(raft) = leader {
                    let key_idx = rng.gen_range(0..1000);
                    if rng.gen_bool(0.7) { 
                        let req = tests_turmoil::typ::Request {
                            client_id: "workload".to_string(),
                            serial: op_count,
                            key: format!("key-{}", key_idx),
                            value: format!("val-{}", rng.r#gen::<u32>()),
                        };
                        if raft.client_write(req).await.is_ok() { op_count += 1; }
                    } else {
                        if raft.ensure_linearizable(ReadPolicy::LeaseRead).await.is_ok() { op_count += 1; }
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        });
    }

    let mut steps: u64 = 0;
    let mut invariant_checks: u64 = 0;
    let mut violations: Vec<String> = Vec::new();
    let mut unique_states = HashSet::new();
    let mut chaos_rng = StdRng::seed_from_u64(seed.wrapping_add(3000));
    let mut member_rng = StdRng::seed_from_u64(seed.wrapping_add(5000));
    
    let mut sm_history: HashMap<u64, StateMachineData> = HashMap::new();
    let mut active_voters: HashSet<NodeId> = (1..=num_initial_nodes as u64).collect();
    let mut next_node_id = (num_initial_nodes as u64) + 1;

    println!("Starting simulation...");

    loop {
        if !running.load(Ordering::Relaxed) || steps >= config.max_steps {
            break;
        }

        if steps > 0 && steps % membership_interval == 0 {
            let add = active_voters.len() < 3 || (active_voters.len() < 7 && member_rng.gen_bool(0.7));
            if add {
                if next_node_id <= max_potential_nodes {
                    println!("MEMBERSHIP: Requesting add node {}...", next_node_id);
                    active_voters.insert(next_node_id);
                    next_node_id += 1;
                    *next_membership.lock().unwrap() = Some(active_voters.clone());
                }
            } else if active_voters.len() > 3 {
                let victim = *active_voters.iter().next().unwrap();
                println!("MEMBERSHIP: Requesting remove node {}...", victim);
                active_voters.remove(&victim);
                *next_membership.lock().unwrap() = Some(active_voters.clone());
            }
        }

        if steps > 0 && steps % chaos_interval == 0 && chaos_rng.gen_bool(restart_chance) {
            let voters: Vec<_> = active_voters.iter().collect();
            if !voters.is_empty() {
                let victim = **voters.get(chaos_rng.gen_range(0..voters.len())).unwrap();
                tests_turmoil::cluster::restart_node(&mut sim, victim);
            }
        }

        loop {
            match sim.step() {
                Ok(more_tasks) => { if !more_tasks { break; } }
                Err(e) => {
                    println!("Simulation stopped: {}", e);
                    return FuzzResult { steps_completed: steps, invariant_checks, violations, unique_states, cluster_state: cluster_state.clone() };
                }
            }
        }
        steps += 1;

        let (metrics, snapshots) = {
            let state = cluster_state.lock().unwrap();
            (state.get_all_metrics(), state.get_all_full_snapshots())
        };
        invariant_checks += 1;

        if steps % 10 == 0 {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for (id, m) in &metrics {
                id.hash(&mut hasher);
                (m.state as u8).hash(&mut hasher);
                m.vote.leader_id().term.hash(&mut hasher);
                if let Some(applied) = m.last_applied {
                    applied.index().hash(&mut hasher);
                    applied.committed_leader_id().term.hash(&mut hasher);
                }
            }
            unique_states.insert(hasher.finish());
        }

        // DEEP SAFETY: Verify State Machine consistency
        for (id, node_snap) in &snapshots {
            if let Some(applied_id) = node_snap.sm.last_applied {
                let idx = applied_id.index();
                if let Some(canonical_sm) = sm_history.get(&idx) {
                    if &node_snap.sm.data != &canonical_sm.data {
                        violations.push(format!("(sm) Divergence at index {}: node {} has different data than canonical state", idx, id));
                    }
                } else {
                    sm_history.insert(idx, node_snap.sm.clone());
                }
            }
        }

        let res = check_state_invariants(&snapshots);
        if !res.passed { violations.extend(res.violations.iter().map(|v| format!("(state) {}", v))); }

        if steps % 5000 == 0 {
            let leaders: Vec<_> = metrics.iter().filter(|(_, m)| m.state.is_leader()).map(|(id, _)| *id).collect();
            println!("[Step {}] Unique States: {}, Leaders: {:?}, Voters: {:?}", 
                steps, unique_states.len(), leaders, active_voters);
        }
        if !violations.is_empty() { break; }
    }

    FuzzResult {
        steps_completed: steps,
        invariant_checks,
        violations,
        unique_states,
        cluster_state: cluster_state.clone(),
    }
}

fn print_final_sm_data(cluster_state: &Arc<Mutex<tests_turmoil::cluster::ClusterState>>) {
    let state = cluster_state.lock().unwrap();
    println!("Final State Machine Data:");
    let mut nodes: Vec<_> = state.state_machines.keys().collect();
    nodes.sort();
    
    for id in nodes {
        let sm = state.state_machines.get(id).unwrap();
        let data = sm.get_data();
        println!("  Node {}: applied={:?}, data_len={}", id, data.last_applied, data.data.len());
        
        let mut keys: Vec<_> = data.data.keys().collect();
        keys.sort();
        for k in keys {
            println!("    {} => {}", k, data.data.get(k).unwrap());
        }
    }
}
