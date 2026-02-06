//! Detsim Orchestrator
//!
//! Continuously runs the fuzzer, uploads logs to GCS, and tracks crashes.
//!
//! Environment variables:
//!   DETSIM_GCS_BUCKET     - GCS bucket name (required for GCS upload)
//!   DETSIM_MAX_STEPS      - Max steps per fuzzer iteration (default: 100000)
//!   DETSIM_ITERATIONS     - Iterations per fuzzer run (default: 100)
//!   DETSIM_BASE_SEED      - Starting seed (default: random)
//!   DETSIM_FUZZ_BINARY    - Path to fuzz binary (default: "fuzz")
//!   DETSIM_DRY_RUN        - If "true", skip GCS uploads (for local testing)

use std::fs;
use std::process::Command;
use std::time::SystemTime;

use uuid::Uuid;

struct Config {
    gcs_bucket: Option<String>,
    max_steps: u64,
    iterations: u64,
    base_seed: u64,
    fuzz_binary: String,
    dry_run: bool,
    disable_logs: bool,
}

impl Config {
    fn from_env() -> Self {
        let gcs_bucket = std::env::var("DETSIM_GCS_BUCKET").ok();

        let max_steps = std::env::var("DETSIM_MAX_STEPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000);

        let iterations = std::env::var("DETSIM_ITERATIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let base_seed = std::env::var("DETSIM_BASE_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });

        let fuzz_binary = std::env::var("DETSIM_FUZZ_BINARY").unwrap_or_else(|_| "fuzz".to_string());

        let dry_run = std::env::var("DETSIM_DRY_RUN")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let disable_logs = std::env::var("DETSIM_DISABLE_LOGS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        Self {
            gcs_bucket,
            max_steps,
            iterations,
            base_seed,
            fuzz_binary,
            dry_run,
            disable_logs,
        }
    }
}

struct RunResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

fn main() {
    println!("=== Detsim Orchestrator ===");

    let config = Config::from_env();

    println!("GCS Bucket: {:?}", config.gcs_bucket);
    println!("Max steps: {}", config.max_steps);
    println!("Iterations: {}", config.iterations);
    println!("Base seed: {}", config.base_seed);
    println!("Fuzz binary: {}", config.fuzz_binary);
    println!("Dry run: {}", config.dry_run);
    println!("Disable logs: {}", config.disable_logs);
    println!("===========================");
    println!();

    let mut seed = config.base_seed;
    let mut total_runs: u64 = 0;
    let mut total_crashes: u64 = 0;

    loop {
        let run_id = Uuid::new_v4().to_string();
        let crash_file = format!("/tmp/crash-{}.json", run_id);

        println!(
            "[Run {}] seed={} run_id={}",
            total_runs + 1,
            seed,
            &run_id[..8]
        );

        let result = run_fuzzer(&config, seed, &crash_file);

        println!(
            "  exit_code={} stdout_len={} stderr_len={}",
            result.exit_code,
            result.stdout.len(),
            result.stderr.len()
        );

        // Upload logs (always)
        if !config.dry_run && !config.disable_logs {
            upload_run_logs(&config, &run_id, seed, &result);
        }

        // Handle crash
        if result.exit_code != 0 {
            total_crashes += 1;
            println!("  CRASH DETECTED!");

            if let Ok(crash_json) = fs::read_to_string(&crash_file) {
                // Parse and display key info
                if let Ok(crash_data) = serde_json::from_str::<serde_json::Value>(&crash_json) {
                    println!("  iteration_seed: {}", crash_data["iteration_seed"]);
                    println!("  steps_completed: {}", crash_data["steps_completed"]);
                    println!("  violation: {}", crash_data["violation"]);
                    if let Some(cmd) = crash_data["reproduce"]["command"].as_str() {
                        println!("  REPRODUCE: {}", cmd);
                    }
                } else {
                    // Fallback: show raw JSON
                    for line in crash_json.lines().take(10) {
                        println!("    {}", line);
                    }
                }

                if !config.dry_run {
                    upload_crash(&config, &crash_json);
                }
            } else {
                eprintln!("  Warning: Could not read crash file at {}", crash_file);
            }
        }

        // Cleanup
        let _ = fs::remove_file(&crash_file);

        total_runs += 1;
        seed = seed.wrapping_add(1);

        println!(
            "  [Stats] total_runs={} total_crashes={} crash_rate={:.2}%",
            total_runs,
            total_crashes,
            (total_crashes as f64 / total_runs as f64) * 100.0
        );
        println!();
    }
}

fn run_fuzzer(config: &Config, seed: u64, crash_file: &str) -> RunResult {
    let mut command = Command::new(&config.fuzz_binary);
    command.args([
        "--seed",
        &seed.to_string(),
        "--max-steps",
        &config.max_steps.to_string(),
        "--iterations",
        &config.iterations.to_string(),
        "--crash-file",
        crash_file,
    ]);

    if config.disable_logs {
        command.stdout(std::process::Stdio::null());
        command.stderr(std::process::Stdio::null());
    }

    let output = command.output().expect("Failed to execute fuzzer");

    RunResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn upload_run_logs(config: &Config, run_id: &str, seed: u64, result: &RunResult) {
    let Some(bucket) = &config.gcs_bucket else {
        return;
    };

    let tmp_dir = format!("/tmp/run-{}", run_id);
    fs::create_dir_all(&tmp_dir).ok();

    // Write metadata
    let metadata = serde_json::json!({
        "run_id": run_id,
        "seed": seed,
        "exit_code": result.exit_code,
        "max_steps": config.max_steps,
        "timestamp": chrono_now(),
    });
    let metadata_path = format!("{}/metadata.json", tmp_dir);
    fs::write(&metadata_path, serde_json::to_string_pretty(&metadata).unwrap()).ok();

    // Write stdout/stderr
    let stdout_path = format!("{}/stdout.log", tmp_dir);
    let stderr_path = format!("{}/stderr.log", tmp_dir);
    fs::write(&stdout_path, &result.stdout).ok();
    fs::write(&stderr_path, &result.stderr).ok();

    // Upload to GCS
    let gcs_path = format!("gs://{}/runs/{}/", bucket, run_id);
    let status = Command::new("gsutil")
        .args(["-m", "cp", "-r", &format!("{}/*", tmp_dir), &gcs_path])
        .status();

    match status {
        Ok(s) if s.success() => println!("  Uploaded logs to {}", gcs_path),
        Ok(s) => eprintln!("  gsutil failed with status: {}", s),
        Err(e) => eprintln!("  gsutil error: {}", e),
    }

    // Cleanup
    fs::remove_dir_all(&tmp_dir).ok();
}

fn upload_crash(config: &Config, crash_json: &str) {
    let Some(bucket) = &config.gcs_bucket else {
        return;
    };

    // Parse to get iteration_seed for filename
    let crash_data: serde_json::Value = match serde_json::from_str(crash_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("  Failed to parse crash JSON: {}", e);
            return;
        }
    };

    let iteration_seed = crash_data["iteration_seed"]
        .as_u64()
        .unwrap_or(0);

    let tmp_path = format!("/tmp/crash-upload-{}.json", iteration_seed);
    fs::write(&tmp_path, crash_json).ok();

    let gcs_path = format!("gs://{}/crashes/{}.json", bucket, iteration_seed);
    let status = Command::new("gsutil")
        .args(["cp", &tmp_path, &gcs_path])
        .status();

    match status {
        Ok(s) if s.success() => println!("  Uploaded crash to {}", gcs_path),
        Ok(s) => eprintln!("  gsutil failed with status: {}", s),
        Err(e) => eprintln!("  gsutil error: {}", e),
    }

    fs::remove_file(&tmp_path).ok();
}

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without chrono dependency
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = duration.as_secs();
    format!("{}", secs)
}
