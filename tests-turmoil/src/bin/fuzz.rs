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

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
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
}

fn run_fuzz_test(config: &FuzzConfig, seed: u64, running: Arc<AtomicBool>) -> FuzzResult {
    // 1. Initialize RNG from seed to derive ALL parameters
    let mut param_rng = StdRng::seed_from_u64(seed);
    
    let num_nodes = param_rng.gen_range(3..=7);
    let fail_rate = param_rng.gen_range(0.0..0.15);
    let heartbeat_interval = param_rng.gen_range(50..150);
    let election_timeout_min = heartbeat_interval * param_rng.gen_range(2..4);
    let election_timeout_max = election_timeout_min + param_rng.gen_range(100..500);
    
    // Percentages for chaos events
    let restart_chance = param_rng.gen_range(0.01..0.2);
    let chaos_interval = param_rng.gen_range(500..2000);

    println!("Simulation Params: nodes={}, fail_rate={:.1}%, hb={}ms, elect={}..{}ms", 
        num_nodes, fail_rate * 100.0, heartbeat_interval, election_timeout_min, election_timeout_max);

    // 2. Initialize simulation with deterministic RNG
    let sim_rng = Box::new(SmallRng08::seed_from_u64(seed));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3600))
        .fail_rate(fail_rate)
        .enable_random_order()
        .build_with_rng(sim_rng);

    let raft_config = Arc::new(openraft::Config {
        heartbeat_interval,
        election_timeout_min,
        election_timeout_max,
        ..Default::default()
    });

    let (cluster_info, cluster_state) = spawn_cluster(
        &mut sim,
        ClusterConfig {
            num_nodes,
            raft_config: (*raft_config).clone(),
            seed,
        },
    );

    // Add chaos agent (network chaos)
    if true {
        let chaos_seed = seed.wrapping_add(1000);
        sim.client("chaos-agent", async move {
            let mut rng = StdRng::seed_from_u64(chaos_seed);
            loop {
                let delay = rng.gen_range(1000..5000);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let chaos_type = rng.gen_range(0..5);
                match chaos_type {
                    0 => {
                        let victim = rng.gen_range(1..=num_nodes);
                        let victim_name = format!("node-{}", victim);
                        tracing::info!("CHAOS: partitioning {}", victim_name);
                        for i in 1..=num_nodes {
                            if i != victim {
                                turmoil::partition(victim_name.clone(), format!("node-{}", i));
                            }
                        }
                    }
                    1 => {
                        tracing::info!("CHAOS: repairing all partitions");
                        for i in 1..=num_nodes {
                            for j in (i + 1)..=num_nodes {
                                turmoil::repair(format!("node-{}", i), format!("node-{}", j));
                            }
                        }
                    }
                    2 => {
                        let a = rng.gen_range(1..=num_nodes);
                        let mut b = rng.gen_range(1..=num_nodes);
                        while b == a { b = rng.gen_range(1..=num_nodes); }
                        tracing::info!("CHAOS: holding messages node-{} <-> node-{}", a, b);
                        turmoil::hold(format!("node-{}", a), format!("node-{}", b));
                    }
                    3 => {
                        tracing::info!("CHAOS: releasing all message holds");
                        for i in 1..=num_nodes {
                            for j in 1..=num_nodes {
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

    // Add workload client
    {
        let cluster_state_clone = cluster_state.clone();
        let workload_seed = seed.wrapping_add(2000);
        sim.client("workload", async move {
            let mut rng = StdRng::seed_from_u64(workload_seed);
            let mut op_count = 0u64;
            loop {
                let delay = rng.gen_range(10..50);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let leader = {
                    let state = cluster_state_clone.lock().unwrap();
                    state.rafts.iter()
                        .find(|(_, raft)| {
                            use openraft::async_runtime::WatchReceiver;
                            raft.metrics().borrow_watched().state.is_leader()
                        })
                        .map(|(&id, raft)| (id, raft.clone()))
                };

                if let Some((leader_id, raft)) = leader {
                    if rng.gen_bool(0.5) {
                        let req = tests_turmoil::typ::Request {
                            client_id: "workload".to_string(),
                            serial: op_count,
                            key: format!("key-{}", op_count % 100),
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

    loop {
        if !running.load(Ordering::Relaxed) || steps >= config.max_steps {
            break;
        }

        // Periodic node restarts
        if steps > 0 && steps % chaos_interval == 0 && chaos_rng.gen_bool(restart_chance) {
            let victim = chaos_rng.gen_range(1..=(num_nodes as u64));
            tests_turmoil::cluster::restart_node(&mut sim, victim);
        }

        match sim.step() {
            Ok(_) => steps += 1,
            Err(_) => break,
        }

        let (metrics, snapshots) = {
            let state = cluster_state.lock().unwrap();
            (state.get_all_metrics(), state.get_all_state_snapshots())
        };
        invariant_checks += 1;

        // FEEDBACK: Compute cluster state hash
        if steps % 10 == 0 {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for (id, m) in &metrics {
                id.hash(&mut hasher);
                (m.state as u8).hash(&mut hasher); // Cast enum to u8 for hashing
                m.vote.leader_id().term.hash(&mut hasher);
                if let Some(applied) = m.last_applied {
                    applied.index().hash(&mut hasher);
                    applied.committed_leader_id().term.hash(&mut hasher);
                }
            }
            unique_states.insert(hasher.finish());
        }

        let res = check_metrics_invariants(&metrics);
        if !res.passed { violations.extend(res.violations.iter().map(|v| format!("(metrics) {}", v))); }

        let res = check_state_invariants(&snapshots);
        if !res.passed { violations.extend(res.violations.iter().map(|v| format!("(state) {}", v))); }

        if steps % 5000 == 0 {
            let leaders: Vec<_> = metrics.iter().filter(|(_, m)| m.state.is_leader()).map(|(id, _)| *id).collect();
            println!("[Step {}] Unique States: {}, Leaders: {:?}", 
                steps, unique_states.len(), leaders);
        }
        
        if !violations.is_empty() { break; }
    }

    FuzzResult {
        steps_completed: steps,
        invariant_checks,
        violations,
        unique_states,
    }
}
