//! Tick-based fuzzer for OpenRaft turmoil tests.
//!
//! Modes:
//!   Fuzz mode:      fuzz --seed <SEED> --max-steps <N> [--crash-file <PATH>]
//!   Reproduce mode: fuzz --reproduce <ITERATION_SEED> --max-steps <N> [--crash-file <PATH>]
//!
//! In fuzz mode, runs multiple iterations with state space exploration.
//! In reproduce mode, runs a single iteration with exact seed for debugging.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use tests_turmoil::cluster::{register_node_storage, spawn_host, ClusterInfo, ClusterState};
use tests_turmoil::invariants::check_state_invariants;
use tests_turmoil::typ::*;

/// Config derived deterministically from a seed
#[derive(Debug, Clone)]
struct DerivedConfig {
    num_initial_nodes: usize,
    max_potential_nodes: u64,
    fail_rate: f64,
    heartbeat_interval: u64,
    election_timeout_min: u64,
    election_timeout_max: u64,
    enable_chaos: bool,
    restart_chance: f64,
    chaos_interval: u64,
    membership_interval: u64,
}

impl DerivedConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let heartbeat_interval = 50 + rng.gen_range(0..100);
        let election_timeout_min = heartbeat_interval * rng.gen_range(2..4);
        Self {
            num_initial_nodes: 3 + rng.gen_range(0..3),            // 3-5 nodes initially
            max_potential_nodes: 10,                               // Max nodes for membership changes
            fail_rate: rng.gen_range(0.0..0.08),                   // 0-8%
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max: election_timeout_min + rng.gen_range(100..500),
            enable_chaos: rng.gen_bool(0.8),                       // 80% chance of chaos
            restart_chance: rng.gen_range(0.01..0.05),             // 1-5% restart chance
            chaos_interval: rng.gen_range(2000..5000),             // Chaos every 2-5k steps
            membership_interval: rng.gen_range(10000..25000),      // Membership change every 10-25k steps
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "num_initial_nodes": self.num_initial_nodes,
            "max_potential_nodes": self.max_potential_nodes,
            "fail_rate": self.fail_rate,
            "heartbeat_interval": self.heartbeat_interval,
            "election_timeout_min": self.election_timeout_min,
            "election_timeout_max": self.election_timeout_max,
            "enable_chaos": self.enable_chaos,
            "restart_chance": self.restart_chance,
            "chaos_interval": self.chaos_interval,
            "membership_interval": self.membership_interval
        })
    }
}

/// Fuzzer configuration from command line
struct FuzzConfig {
    /// Base seed for fuzz mode
    base_seed: Option<u64>,
    /// Exact seed for reproduce mode
    reproduce_seed: Option<u64>,
    /// Maximum steps per iteration
    max_steps: u64,
    /// Number of iterations to run (0 = forever)
    iterations: u64,
    /// Path to write crash info
    crash_file: Option<String>,
}

impl Default for FuzzConfig {
    fn default() -> Self {
        Self {
            base_seed: None,
            reproduce_seed: None,
            max_steps: 100_000,
            iterations: 100, // Default to 100 iterations per batch
            crash_file: None,
        }
    }
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("openraft=trace".parse().unwrap())
                .add_directive("tests_turmoil=debug".parse().unwrap())
                .add_directive("info".parse().unwrap()), // Default to info for others
        )
        .init();

    // Parse args
    let args: Vec<String> = std::env::args().collect();
    let mut config = FuzzConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" | "-s" => {
                i += 1;
                config.base_seed = Some(args[i].parse().expect("Invalid seed"));
            }
            "--reproduce" | "-r" => {
                i += 1;
                config.reproduce_seed = Some(args[i].parse().expect("Invalid reproduce seed"));
            }
            "--max-steps" | "--steps" => {
                i += 1;
                config.max_steps = args[i].parse().expect("Invalid max-steps");
            }
            "--iterations" | "-i" => {
                i += 1;
                config.iterations = args[i].parse().expect("Invalid iterations");
            }
            "--crash-file" => {
                i += 1;
                config.crash_file = Some(args[i].clone());
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Dispatch to appropriate mode
    if let Some(iteration_seed) = config.reproduce_seed {
        run_reproduce_mode(iteration_seed, config.max_steps, config.crash_file);
    } else {
        let base_seed = config.base_seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
        run_fuzz_mode(base_seed, config.max_steps, config.iterations, config.crash_file);
    }
}

fn print_help() {
    println!("OpenRaft Turmoil Fuzzer (tick-based)");
    println!();
    println!("MODES:");
    println!();
    println!("  Fuzz mode (run multiple iterations with state exploration):");
    println!("    fuzz --seed <SEED> --max-steps <N> [--iterations <N>] [--crash-file <PATH>]");
    println!();
    println!("  Reproduce mode (run single iteration with exact seed):");
    println!("    fuzz --reproduce <ITERATION_SEED> --max-steps <N> [--crash-file <PATH>]");
    println!();
    println!("OPTIONS:");
    println!("  -s, --seed <SEED>           Base RNG seed for fuzzing [default: random]");
    println!("  -r, --reproduce <SEED>      Exact iteration seed to reproduce");
    println!("      --max-steps <N>         Max steps per iteration [default: 100000]");
    println!("  -i, --iterations <N>        Number of iterations (0 = forever) [default: 100]");
    println!("      --crash-file <PATH>     Where to write crash info (fuzz mode only)");
    println!("  -h, --help                  Show this help");
}

/// Reproduce mode: run a single iteration with the exact seed
fn run_reproduce_mode(iteration_seed: u64, max_steps: u64, crash_file: Option<String>) {
    println!("=== OpenRaft Fuzzer REPRODUCE MODE ===");
    println!("Iteration seed: {}", iteration_seed);
    println!("Max steps: {}", max_steps);
    println!("Crash file: {:?}", crash_file);

    let derived = DerivedConfig::from_seed(iteration_seed);
    println!();
    println!("Derived config:");
    println!("  num_initial_nodes: {}", derived.num_initial_nodes);
    println!("  max_potential_nodes: {}", derived.max_potential_nodes);
    println!("  fail_rate: {:.4}", derived.fail_rate);
    println!("  heartbeat_interval: {}ms", derived.heartbeat_interval);
    println!("  election_timeout_min: {}ms", derived.election_timeout_min);
    println!("  election_timeout_max: {}ms", derived.election_timeout_max);
    println!("  enable_chaos: {}", derived.enable_chaos);
    println!("  restart_chance: {:.4}", derived.restart_chance);
    println!("  chaos_interval: {}", derived.chaos_interval);
    println!("  membership_interval: {}", derived.membership_interval);
    println!("======================================");
    println!();

    // Set up Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    ctrlc::set_handler(move || {
        println!("\n\nInterrupted!");
        running_clone.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let result = run_single_iteration(iteration_seed, &derived, max_steps, running);

    if !result.violations.is_empty() {
        // Print failure info
        println!();
        println!("=== REPRODUCED FAILURE ===");
        println!("Steps completed: {}", result.steps_completed);
        println!("Invariant checks: {}", result.invariant_checks);
        println!();
        println!("Violations:");
        for v in &result.violations {
            println!("  - {}", v);
        }
        println!();
        println!("REPRODUCE WITH:");
        println!(
            "  cargo run --bin fuzz -- --reproduce {} --max-steps {}",
            iteration_seed, max_steps
        );

        // Write crash file
        if let Some(path) = &crash_file {
            let crash_info = serde_json::json!({
                "base_seed": iteration_seed, // In reproduce mode, base_seed is iteration_seed
                "iteration": 0, // In reproduce mode, it's always the first (0th) iteration
                "iteration_seed": iteration_seed,
                "max_steps": max_steps,
                "steps_completed": result.steps_completed,
                "violation": result.violations.first(),
                "config": derived.to_json(),
                "reproduce": {
                    "command": format!("cargo run --bin fuzz -- --reproduce {} --max-steps {} --crash-file {}", iteration_seed, max_steps, path),
                    "iteration_seed": iteration_seed,
                    "max_steps": max_steps
                }
            });
            if let Err(e) = fs::write(path, serde_json::to_string_pretty(&crash_info).unwrap()) {
                eprintln!("Failed to write crash file: {}", e);
            }
        }

        std::process::exit(1);
    }

    println!();
    println!("=== Results ===");
    println!("Steps completed: {}", result.steps_completed);
    println!("Invariant checks: {}", result.invariant_checks);
    println!("Status: PASSED (no violation found)");
}

/// Fuzz mode: run multiple iterations with state space exploration
fn run_fuzz_mode(base_seed: u64, max_steps: u64, iterations: u64, crash_file: Option<String>) {
    println!("=== OpenRaft Turmoil Fuzzer (tick-based) ===");
    println!("Base seed: {}", base_seed);
    println!("Max steps/iter: {}", max_steps);
    println!("Iterations: {} (0 = forever)", iterations);
    println!("Crash file: {:?}", crash_file);
    println!("============================================");
    println!();

    // Set up Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    ctrlc::set_handler(move || {
        println!("\n\nInterrupted!");
        running_clone.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let mut iteration = 0u64;
    let mut total_steps = 0u64;
    let mut total_checks = 0u64;

    loop {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        // Check iteration limit (0 = forever)
        if iterations > 0 && iteration >= iterations {
            break;
        }

        let iteration_seed = base_seed.wrapping_add(iteration);
        let derived = DerivedConfig::from_seed(iteration_seed);

        println!(
            "--- Iteration {} (seed: {}, nodes: {}, fail_rate: {:.2}%, chaos: {}) ---",
            iteration + 1,
            iteration_seed,
            derived.num_initial_nodes,
            derived.fail_rate * 100.0,
            derived.enable_chaos
        );

        let result = run_single_iteration(iteration_seed, &derived, max_steps, running.clone());
        total_steps += result.steps_completed;
        total_checks += result.invariant_checks;

        if !result.violations.is_empty() {
            // Print failure info
            println!();
            println!("=== FAILED at iteration {} ===", iteration + 1);
            println!("Iteration seed: {}", iteration_seed);
            println!("Steps completed: {}", result.steps_completed);
            println!("Invariant checks: {}", result.invariant_checks);
            println!();
            println!("Derived config:");
            println!("  num_initial_nodes: {}", derived.num_initial_nodes);
            println!("  max_potential_nodes: {}", derived.max_potential_nodes);
            println!("  fail_rate: {:.4}", derived.fail_rate);
            println!("  heartbeat_interval: {}ms", derived.heartbeat_interval);
            println!("  election_timeout_min: {}ms", derived.election_timeout_min);
            println!("  election_timeout_max: {}ms", derived.election_timeout_max);
            println!("  enable_chaos: {}", derived.enable_chaos);
            println!("  restart_chance: {:.4}", derived.restart_chance);
            println!("  chaos_interval: {}", derived.chaos_interval);
            println!("  membership_interval: {}", derived.membership_interval);
            println!();
            println!("Violations:");
            for v in &result.violations {
                println!("  - {}", v);
            }
            println!();
            println!("REPRODUCE WITH:");
            println!(
                "  cargo run --bin fuzz -- --reproduce {} --max-steps {}",
                iteration_seed, max_steps
            );

            // Write crash file
            if let Some(path) = &crash_file {
                let crash_info = serde_json::json!({
                    "base_seed": base_seed,
                    "iteration": iteration,
                    "iteration_seed": iteration_seed,
                    "max_steps": max_steps,
                    "steps_completed": result.steps_completed,
                    "violation": result.violations.first(),
                    "config": derived.to_json(),
                    "reproduce": {
                        "command": format!("cargo run --bin fuzz -- --reproduce {} --max-steps {} --crash-file {}", iteration_seed, max_steps, path),
                        "iteration_seed": iteration_seed,
                        "max_steps": max_steps
                    }
                });
                if let Err(e) = fs::write(path, serde_json::to_string_pretty(&crash_info).unwrap()) {
                    eprintln!("Failed to write crash file: {}", e);
                }
            }

            std::process::exit(1);
        }

        iteration += 1;
    }

    // Print success results
    println!();
    println!("=== Results ===");
    println!("Iterations completed: {}", iteration);
    println!("Total steps: {}", total_steps);
    println!("Total invariant checks: {}", total_checks);
    println!("Status: PASSED");
}

struct FuzzResult {
    steps_completed: u64,
    invariant_checks: u64,
    violations: Vec<String>,
}

fn run_single_iteration(
    iteration_seed: u64,
    derived: &DerivedConfig,
    max_steps: u64,
    running: Arc<AtomicBool>,
) -> FuzzResult {
    let rng = Box::new(SmallRng::seed_from_u64(iteration_seed));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3600))
        .fail_rate(derived.fail_rate)
        .enable_random_order()
        .tcp_capacity(65536)
        .build_with_rng(rng);

    let raft_config = Arc::new(openraft::Config {
        heartbeat_interval: derived.heartbeat_interval,
        election_timeout_min: derived.election_timeout_min,
        election_timeout_max: derived.election_timeout_max,
        ..Default::default()
    });

    let cluster_state = Arc::new(Mutex::new(ClusterState::new()));
    let next_membership = Arc::new(Mutex::new(None::<HashSet<NodeId>>));

    // Pre-register ALL potential hosts for dynamic membership
    let mut all_possible_nodes = BTreeMap::new();
    for id in 1..=derived.max_potential_nodes {
        all_possible_nodes.insert(id, Node { addr: format!("{}:9000", ClusterInfo::host_name(id)) });
    }

    for id in 1..=derived.max_potential_nodes {
        register_node_storage(id, &cluster_state);
        spawn_host(&mut sim, id, raft_config.clone(), cluster_state.clone(), iteration_seed, all_possible_nodes.clone());
    }

    // Add chaos agent if enabled
    if derived.enable_chaos {
        let chaos_seed = iteration_seed.wrapping_add(1000);
        let max_nodes = derived.max_potential_nodes;

        sim.client("chaos-agent", async move {
            let mut rng = StdRng::seed_from_u64(chaos_seed);

            loop {
                let delay = rng.gen_range(1000..5000);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let chaos_type = rng.gen_range(0..5);
                match chaos_type {
                    0 => {
                        // Partition a random node
                        let victim = rng.gen_range(1..=max_nodes);
                        let victim_name = format!("node-{}", victim);
                        for i in 1..=max_nodes {
                            if i != victim {
                                turmoil::partition(victim_name.clone(), format!("node-{}", i));
                            }
                        }
                    }
                    1 => {
                        // Repair all partitions
                        for i in 1..=max_nodes {
                            for j in (i + 1)..=max_nodes {
                                turmoil::repair(format!("node-{}", i), format!("node-{}", j));
                            }
                        }
                    }
                    2 => {
                        // Hold messages between two random nodes
                        let a = rng.gen_range(1..=max_nodes);
                        let mut b = rng.gen_range(1..=max_nodes);
                        while b == a {
                            b = rng.gen_range(1..=max_nodes);
                        }
                        turmoil::hold(format!("node-{}", a), format!("node-{}", b));
                    }
                    3 => {
                        // Release all holds
                        for i in 1..=max_nodes {
                            for j in 1..=max_nodes {
                                if i != j {
                                    turmoil::release(format!("node-{}", i), format!("node-{}", j));
                                }
                            }
                        }
                    }
                    _ => {
                        // Do nothing - let system stabilize
                    }
                }
            }

            #[allow(unreachable_code)]
            Ok(())
        });
    }

    // Add membership-agent for executing membership changes
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
        let workload_seed = iteration_seed.wrapping_add(2000);

        sim.client("workload", async move {
            let mut rng = StdRng::seed_from_u64(workload_seed);
            let mut write_count = 0u64;

            loop {
                let delay = rng.gen_range(10..50);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let leader = {
                    let state = cluster_state_clone.lock().unwrap();
                    state
                        .rafts
                        .iter()
                        .find(|(_, raft)| {
                            use openraft::async_runtime::WatchReceiver;
                            raft.metrics().borrow_watched().state.is_leader()
                        })
                        .map(|(_, raft)| raft.clone())
                };

                if let Some(raft) = leader {
                    let key = format!("key-{}", write_count % 1000);
                    let value = format!("value-{}-{}", write_count, rng.r#gen::<u32>());

                    let req = Request {
                        client_id: "workload".to_string(),
                        serial: write_count,
                        key,
                        value,
                    };
                    if raft.client_write(req).await.is_ok() {
                        write_count += 1;
                    }
                }
            }

            #[allow(unreachable_code)]
            Ok(())
        });
    }

    // Main loop: step simulation and check invariants
    let mut steps: u64 = 0;
    let mut invariant_checks: u64 = 0;
    let mut violations: Vec<String> = Vec::new();
    let mut chaos_rng = StdRng::seed_from_u64(iteration_seed.wrapping_add(3000));
    let mut member_rng = StdRng::seed_from_u64(iteration_seed.wrapping_add(5000));

    let mut active_voters: HashSet<NodeId> = (1..=derived.num_initial_nodes as u64).collect();
    let mut next_node_id = (derived.num_initial_nodes as u64) + 1;

    println!("Starting simulation...");

    loop {
        if !running.load(Ordering::Relaxed) {
            println!("Interrupted at step {}", steps);
            break;
        }

        if steps >= max_steps {
            println!("Reached max steps: {}", max_steps);
            break;
        }

        // Membership changes
        if steps > 0 && steps % derived.membership_interval == 0 {
            let add = active_voters.len() < 3 || (active_voters.len() < 7 && member_rng.gen_bool(0.7));
            if add {
                if next_node_id <= derived.max_potential_nodes {
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

        // Crash restarts
        if steps > 0 && steps % derived.chaos_interval == 0 && chaos_rng.gen_bool(derived.restart_chance) {
            let voters: Vec<_> = active_voters.iter().collect();
            if !voters.is_empty() {
                let victim = **voters.get(chaos_rng.gen_range(0..voters.len())).unwrap();
                tests_turmoil::cluster::restart_node(&mut sim, victim);
            }
        }

        // Step simulation until no more tasks
        loop {
            match sim.step() {
                Ok(more_tasks) => {
                    if !more_tasks {
                        break;
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("duration") || err_str.contains("without completing") {
                        println!("Simulation duration reached at step {}", steps);
                    } else {
                        println!("Simulation error at step {}: {}", steps, e);
                    }
                    return FuzzResult {
                        steps_completed: steps,
                        invariant_checks,
                        violations,
                    };
                }
            }
        }
        steps += 1;

        // Check invariants after every step
        let (metrics, snapshots) = {
            let state = cluster_state.lock().unwrap();
            (state.get_all_metrics(), state.get_all_full_snapshots())
        };
        invariant_checks += 1;
        let result = check_state_invariants(&snapshots);
        if !result.passed {
            for v in &result.violations {
                let msg = format!("Step {}: {:?}", steps, v);
                println!("VIOLATION: {}", msg);
                violations.push(msg);
            }
            return FuzzResult {
                steps_completed: steps,
                invariant_checks,
                violations,
            };
        }

        // Progress report every 5000 steps
        if steps % 5000 == 0 {
            let leaders: Vec<_> = metrics
                .iter()
                .filter(|(_, m)| m.state.is_leader())
                .map(|(id, _)| *id)
                .collect();
            let max_term = metrics
                .iter()
                .map(|(_, m)| m.vote.leader_id().term)
                .max()
                .unwrap_or(0);

            println!(
                "[Step {}] leaders={:?}, term={}, voters={:?}, checks={}",
                steps,
                leaders,
                max_term,
                active_voters,
                invariant_checks
            );
        }

        if !violations.is_empty() {
            break;
        }
    }

    FuzzResult {
        steps_completed: steps,
        invariant_checks,
        violations,
    }
}