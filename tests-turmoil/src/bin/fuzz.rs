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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::ReadPolicy;
use rand_09::rngs::StdRng;
use rand_09::{Rng, SeedableRng};
use rand::rngs::SmallRng as SmallRng08;
use rand::SeedableRng as SeedableRng08;

use tests_turmoil::cluster::{spawn_cluster, ClusterConfig};
use tests_turmoil::invariants::check_metrics_invariants;
use tests_turmoil::invariants::check_state_invariants;

/// Fuzzer configuration
struct FuzzConfig {
    /// RNG seed for reproducibility (None = random)
    seed: Option<u64>,
    /// Number of iterations to run (0 = forever)
    iterations: u64,
    /// Number of nodes in cluster
    num_nodes: usize,
    /// Message failure rate (0.0 to 1.0)
    fail_rate: f64,
    /// Enable network chaos (partitions, holds)
    enable_chaos: bool,
    /// Maximum number of steps per iteration
    max_steps: u64,
}

impl Default for FuzzConfig {
    fn default() -> Self {
        Self {
            seed: None, // Random
            iterations: 0, // Forever
            num_nodes: 5,
            fail_rate: 0.05,
            enable_chaos: true,
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
            "--nodes" | "-n" => {
                i += 1;
                config.num_nodes = args[i].parse().expect("Invalid node count");
            }
            "--fail-rate" | "-f" => {
                i += 1;
                config.fail_rate = args[i].parse().expect("Invalid fail rate");
            }
            "--steps" => {
                i += 1;
                config.max_steps = args[i].parse().expect("Invalid steps");
            }
            "--no-chaos" => {
                config.enable_chaos = false;
            }
            "--help" | "-h" => {
                println!("OpenRaft Turmoil Fuzzer (tick-based)");
                println!();
                println!("Usage: fuzz [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -s, --seed <SEED>          RNG seed for reproducibility [default: random]");
                println!("  -i, --iterations <N>       Number of iterations (0 = forever) [default: 0]");
                println!("  -n, --nodes <N>            Number of nodes [default: 5]");
                println!("  -f, --fail-rate <RATE>     Message failure rate 0.0-1.0 [default: 0.05]");
                println!("      --steps <N>            Steps per iteration [default: 100000]");
                println!("      --no-chaos             Disable chaos injection");
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

    println!("=== OpenRaft Turmoil Fuzzer (tick-based) ===");
    println!("Start seed: {}", start_seed);
    println!("Iterations: {} (0 = forever)", config.iterations);
    println!("Nodes: {}", config.num_nodes);
    println!("Fail rate: {:.1}%", config.fail_rate * 100.0);
    println!("Steps/iter: {}", config.max_steps);
    println!("Chaos: {}", config.enable_chaos);
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

    // Run iterations
    let mut iteration = 0u64;
    let mut total_steps = 0u64;
    let mut total_checks = 0u64;

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

        if !result.violations.is_empty() {
            // Print failure results
            println!();
            println!("=== FAILED at iteration {} ===", iteration + 1);
            println!("Failing seed: {}", seed);
            println!("Steps completed: {}", result.steps_completed);
            println!("Invariant checks: {}", result.invariant_checks);
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

fn run_fuzz_test(config: &FuzzConfig, seed: u64, running: Arc<AtomicBool>) -> FuzzResult {
    let rng = Box::new(SmallRng08::seed_from_u64(seed));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3600)) // Long duration, we control via steps
        .fail_rate(config.fail_rate)
        .enable_random_order()
        .build_with_rng(rng);

    // Randomize Raft config based on seed
    let raft_config = Arc::new(openraft::Config {
        heartbeat_interval: 100 + (seed % 100),
        election_timeout_min: 500 + (seed % 200),
        election_timeout_max: 1000 + (seed % 300),
        ..Default::default()
    });

    let (cluster_info, cluster_state) = spawn_cluster(
        &mut sim,
        ClusterConfig {
            num_nodes: config.num_nodes,
            raft_config: (*raft_config).clone(),
            seed,
        },
    );

    // Add chaos agent if enabled (runs inside simulation)
    if config.enable_chaos {
        let chaos_seed = seed.wrapping_add(1000);
        let num_nodes = config.num_nodes;
        let _cluster_info = cluster_info.clone();
        let _cluster_state = cluster_state.clone();
        let _raft_config = raft_config.clone();

        sim.client("chaos-agent", async move {
            let mut rng = StdRng::seed_from_u64(chaos_seed);

            loop {
                // Random delay between chaos events (longer stability windows)
                let delay = rng.gen_range(1000..5000);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let chaos_type = rng.gen_range(0..8);
                match chaos_type {
                    0 => {
                        // Partition a random node
                        let victim = rng.gen_range(1..=num_nodes);
                        let victim_name = format!("node-{}", victim);
                        tracing::info!("CHAOS: partitioning {}", victim_name);
                        for i in 1..=num_nodes {
                            if i != (victim as usize) {
                                let other = format!("node-{}", i);
                                turmoil::partition(victim_name.clone(), other);
                            }
                        }
                    }
                    1 => {
                        // Repair all partitions
                        tracing::info!("CHAOS: repairing all partitions");
                        for i in 1..=num_nodes {
                            for j in (i + 1)..=num_nodes {
                                let a = format!("node-{}", i);
                                let b = format!("node-{}", j);
                                turmoil::repair(a, b);
                            }
                        }
                    }
                    2 => {
                        // Hold messages between two random nodes
                        let a = rng.gen_range(1..=num_nodes);
                        let mut b = rng.gen_range(1..=num_nodes);
                        while b == a {
                            b = rng.gen_range(1..=num_nodes);
                        }
                        tracing::info!("CHAOS: holding messages between node-{} and node-{}", a, b);
                        turmoil::hold(format!("node-{}", a), format!("node-{}", b));
                    }
                    3 => {
                        // Release all holds
                        tracing::info!("CHAOS: releasing all message holds");
                        for i in 1..=num_nodes {
                            for j in 1..=num_nodes {
                                if i != j {
                                    turmoil::release(format!("node-{}", i), format!("node-{}", j));
                                }
                            }
                        }
                    }
                    4 => {
                        // Restart a random node
                        let _victim = rng.gen_range(1..=(num_nodes as u64));
                        // Handled in main loop for simplicity with Sim access
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

    // Add workload client (runs inside simulation)
    {
        let cluster_state_clone = cluster_state.clone();
        let workload_seed = seed.wrapping_add(2000);

        sim.client("workload", async move {
            let mut rng = StdRng::seed_from_u64(workload_seed);
            let mut op_count = 0u64;

            loop {
                // Use a modest delay in virtual time (10-50ms)
                let delay = rng.gen_range(10..50);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                // Find leader
                let leader = {
                    let state = cluster_state_clone.lock().unwrap();
                    state
                        .rafts
                        .iter()
                        .find(|(_, raft)| {
                            use openraft::async_runtime::WatchReceiver;
                            raft.metrics().borrow_watched().state.is_leader()
                        })
                        .map(|(&id, raft)| (id, raft.clone()))
                };

                if let Some((leader_id, raft)) = leader {
                    let do_write = rng.gen_bool(0.5);

                    if do_write {
                        let key = format!("key-{}", op_count % 100);
                        let value = format!("value-{}-{}", op_count, rng.r#gen::<u32>());

                        let req = tests_turmoil::typ::Request {
                            client_id: "workload".to_string(),
                            serial: op_count,
                            key: key.clone(),
                            value: value.clone(),
                        };
                        tracing::info!("WORKLOAD: writing {}={} to leader {}", key, value, leader_id);
                        if raft.client_write(req).await.is_ok() {
                            tracing::info!("WORKLOAD: write {} successful", op_count);
                            op_count += 1;
                        } else {
                            tracing::warn!("WORKLOAD: write {} failed", op_count);
                        }
                    } else {
                        // Linearizable read
                        tracing::info!("WORKLOAD: reading from leader {}", leader_id);
                        match raft.ensure_linearizable(ReadPolicy::LeaseRead).await {
                            Ok(_) => {
                                // Once leadership is confirmed, we can read from the state machine.
                                // In a real app, you'd query your state machine here.
                                // Here we just log success.
                                tracing::info!("WORKLOAD: linearizable read successful from leader {}", leader_id);
                                op_count += 1;
                            }
                            Err(e) => {
                                tracing::warn!("WORKLOAD: linearizable read failed from leader {}: {}", leader_id, e);
                            }
                        }
                    }
                } else {
                    tracing::info!("WORKLOAD: no leader found");
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
    let mut chaos_rng = StdRng::seed_from_u64(seed.wrapping_add(3000));
    let raft_config_arc = raft_config.clone();

    println!("Starting simulation...");

    loop {
        // Check if we should stop
        if !running.load(Ordering::Relaxed) {
            println!("Interrupted at step {}", steps);
            break;
        }

        if steps >= config.max_steps {
            println!("Reached max steps: {}", config.max_steps);
            break;
        }

        // Randomly restart a node (external chaos)
        if config.enable_chaos && steps > 0 && steps % 1000 == 0 && chaos_rng.gen_bool(0.1) {
            let victim = chaos_rng.gen_range(1..=(config.num_nodes as u64));
            tests_turmoil::cluster::restart_node(&mut sim, victim);
        }

        // Step the simulation - keep stepping even when Ok(false)
        // turmoil returns Ok(false) when tasks are waiting, but time still advances
        match sim.step() {
            Ok(_) => {
                steps += 1;
            }
            Err(e) => {
                // Check if it's just "simulation duration exceeded" which is expected
                let err_str = e.to_string();
                if err_str.contains("duration") || err_str.contains("without completing") {
                    println!("Simulation duration reached at step {}", steps);
                } else {
                    println!("Simulation error at step {}: {}", steps, e);
                }
                break;
            }
        }

        // ========== CHECK INVARIANTS (after every step) ==========
        let (metrics, snapshots) = {
            let state = cluster_state.lock().unwrap();
            (state.get_all_metrics(), state.get_all_state_snapshots())
        };
        invariant_checks += 1;

        // Check metrics invariants
        let result = check_metrics_invariants(&metrics);
        if !result.passed {
            for v in &result.violations {
                let msg = format!("Step {}: (metrics) {}", steps, v);
                println!("VIOLATION: {}", msg);
                violations.push(msg);
            }
        }

        // Check internal state invariants
        let result = check_state_invariants(&snapshots);
        if !result.passed {
            for v in &result.violations {
                let msg = format!("Step {}: (state) {}", steps, v);
                println!("VIOLATION: {}", msg);
                violations.push(msg);
            }
        }
        // ===========================================================

        // Progress report every 1000 steps
        if steps % 1000 == 0 {
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
                "[Step {}] leaders={:?}, term={}, checks={}, violations={}",
                steps, leaders, max_term, invariant_checks, violations.len()
            );
        }
    }

    FuzzResult {
        steps_completed: steps,
        invariant_checks,
        violations,
    }
}
