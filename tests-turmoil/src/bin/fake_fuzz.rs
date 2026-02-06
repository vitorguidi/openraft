//! Fake fuzz binary that fails 5% of the time.
//! For testing the orchestrator in isolation.
//!
//! Modes:
//!   Fuzz mode:      fake_fuzz --seed <SEED> --max-steps <STEPS> --crash-file <PATH>
//!   Reproduce mode: fake_fuzz --reproduce <ITERATION_SEED> --max-steps <STEPS>

use rand::Rng;
use rand::SeedableRng;
use std::fs;

/// Config derived deterministically from a seed
#[derive(Debug, Clone)]
struct DerivedConfig {
    nodes: usize,
    fail_rate: f64,
    heartbeat_interval: u64,
    election_timeout_min: u64,
    election_timeout_max: u64,
}

impl DerivedConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        Self {
            nodes: 3 + rng.gen_range(0..5),                    // 3-7 nodes
            fail_rate: rng.gen_range(0.0..0.15),               // 0-15%
            heartbeat_interval: 50 + rng.gen_range(0..50),     // 50-100ms
            election_timeout_min: 150 + rng.gen_range(0..100), // 150-250ms
            election_timeout_max: 300 + rng.gen_range(0..200), // 300-500ms
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "nodes": self.nodes,
            "fail_rate": self.fail_rate,
            "heartbeat_interval": self.heartbeat_interval,
            "election_timeout_min": self.election_timeout_min,
            "election_timeout_max": self.election_timeout_max
        })
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut base_seed: Option<u64> = None;
    let mut reproduce_seed: Option<u64> = None;
    let mut max_steps: u64 = 100000;
    let mut crash_file: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" | "-s" => {
                i += 1;
                base_seed = Some(args[i].parse().expect("Invalid seed"));
            }
            "--reproduce" | "-r" => {
                i += 1;
                reproduce_seed = Some(args[i].parse().expect("Invalid reproduce seed"));
            }
            "--max-steps" => {
                i += 1;
                max_steps = args[i].parse().unwrap_or(100000);
            }
            "--crash-file" => {
                i += 1;
                crash_file = Some(args[i].clone());
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {}
        }
        i += 1;
    }

    // Dispatch to appropriate mode
    if let Some(seed) = reproduce_seed {
        run_reproduce_mode(seed, max_steps);
    } else if let Some(seed) = base_seed {
        run_fuzz_mode(seed, max_steps, crash_file);
    } else {
        eprintln!("Error: Must provide either --seed (fuzz mode) or --reproduce (reproduce mode)");
        print_help();
        std::process::exit(1);
    }
}

fn print_help() {
    println!("Fake Fuzz - Test harness for orchestrator");
    println!();
    println!("MODES:");
    println!();
    println!("  Fuzz mode (run multiple iterations):");
    println!("    fake_fuzz --seed <SEED> --max-steps <N> [--crash-file <PATH>]");
    println!();
    println!("  Reproduce mode (run single iteration with exact seed):");
    println!("    fake_fuzz --reproduce <ITERATION_SEED> --max-steps <N>");
    println!();
    println!("OPTIONS:");
    println!("  -s, --seed <SEED>           Base RNG seed for fuzzing");
    println!("  -r, --reproduce <SEED>      Exact iteration seed to reproduce");
    println!("      --max-steps <N>         Max steps per iteration [default: 100000]");
    println!("      --crash-file <PATH>     Where to write crash info (fuzz mode only)");
    println!("  -h, --help                  Show this help");
}

/// Reproduce mode: run a single iteration with the exact seed
fn run_reproduce_mode(iteration_seed: u64, max_steps: u64) {
    println!("=== Fake Fuzz REPRODUCE MODE ===");
    println!("Iteration seed: {}", iteration_seed);
    println!("Max steps: {}", max_steps);

    let config = DerivedConfig::from_seed(iteration_seed);
    println!("Derived config: {:?}", config);
    println!("================================");
    println!();

    // Simulate running the iteration
    std::thread::sleep(std::time::Duration::from_millis(100));

    // In reproduce mode, we deterministically "fail" based on seed
    // (same logic as fuzz mode would use for this iteration)
    let mut rng = rand::rngs::StdRng::seed_from_u64(iteration_seed.wrapping_add(999));
    let fail = rng.gen_bool(0.05);

    if fail {
        let steps_completed = rng.gen_range(1000..max_steps.min(50000));
        println!("=== REPRODUCED FAILURE ===");
        println!("Steps completed: {}", steps_completed);
        println!("Violation: MultipleLeadersInTerm {{ term: 5, leaders: [1, 3] }}");
        std::process::exit(1);
    }

    println!("=== Results ===");
    println!("Steps: {}", max_steps);
    println!("Status: PASSED (no violation in reproduce mode)");
}

/// Fuzz mode: run multiple iterations
fn run_fuzz_mode(base_seed: u64, max_steps: u64, crash_file: Option<String>) {
    println!("=== Fake Fuzz FUZZ MODE ===");
    println!("Base seed: {}", base_seed);
    println!("Max steps: {}", max_steps);
    println!("===========================");
    println!();

    // Determine how many iterations to run (and which one fails, if any)
    let mut rng = rand::rngs::StdRng::seed_from_u64(base_seed);
    let num_iterations = rng.gen_range(50..200u64);

    // Simulate running iterations
    std::thread::sleep(std::time::Duration::from_millis(100));

    for iteration in 0..num_iterations {
        let iteration_seed = base_seed.wrapping_add(iteration);

        // Check if this iteration fails (5% chance per iteration)
        let mut iter_rng = rand::rngs::StdRng::seed_from_u64(iteration_seed.wrapping_add(999));
        let fail = iter_rng.gen_bool(0.05);

        if fail {
            let config = DerivedConfig::from_seed(iteration_seed);
            let steps_completed = iter_rng.gen_range(1000..max_steps.min(50000));

            println!();
            println!("=== FAILED at iteration {} ===", iteration);
            println!("Iteration seed: {}", iteration_seed);
            println!("Steps completed: {}", steps_completed);
            println!();
            println!("Derived config for this iteration:");
            println!("  nodes: {}", config.nodes);
            println!("  fail_rate: {:.4}", config.fail_rate);
            println!("  heartbeat_interval: {}ms", config.heartbeat_interval);
            println!("  election_timeout_min: {}ms", config.election_timeout_min);
            println!("  election_timeout_max: {}ms", config.election_timeout_max);
            println!();
            println!("Violations:");
            println!(
                "  - Step {}: MultipleLeadersInTerm {{ term: 5, leaders: [1, 3] }}",
                steps_completed
            );
            println!();
            println!("REPRODUCE WITH:");
            println!(
                "  cargo run --bin fuzz -- --reproduce {} --max-steps {}",
                iteration_seed, max_steps
            );

            if let Some(path) = crash_file {
                let crash_info = serde_json::json!({
                    "base_seed": base_seed,
                    "iteration": iteration,
                    "iteration_seed": iteration_seed,
                    "max_steps": max_steps,
                    "steps_completed": steps_completed,
                    "violation": "MultipleLeadersInTerm { term: 5, leaders: [1, 3] }",
                    "config": config.to_json(),
                    "reproduce": {
                        "command": format!("cargo run --bin fuzz -- --reproduce {} --max-steps {}", iteration_seed, max_steps),
                        "iteration_seed": iteration_seed,
                        "max_steps": max_steps
                    }
                });
                if let Err(e) = fs::write(&path, serde_json::to_string_pretty(&crash_info).unwrap())
                {
                    eprintln!("Failed to write crash file: {}", e);
                }
            }

            std::process::exit(1);
        }
    }

    println!();
    println!("=== Results ===");
    println!("Iterations completed: {}", num_iterations);
    println!("Total steps: ~{}", num_iterations * max_steps);
    println!("Status: PASSED");
}
