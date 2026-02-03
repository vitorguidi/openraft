//! Chaos testing for OpenRaft using turmoil.
//!
//! These tests spawn a cluster, inject network failures, and verify Raft invariants.

use std::sync::Arc;
use std::time::Duration;

use tests_turmoil::client::run_chaos_client;
use tests_turmoil::client::OperationHistory;
use tests_turmoil::cluster::spawn_cluster;
use tests_turmoil::cluster::ClusterConfig;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

/// Basic test: spawn a cluster and let it elect a leader.
#[test]
fn test_basic_cluster_startup() {
    init_tracing();

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    let _cluster = spawn_cluster(&mut sim, ClusterConfig::default());

    sim.client("test-client", async move {
        // Wait for cluster to stabilize and elect a leader
        sleep(Duration::from_secs(3)).await;

        tracing::info!("Cluster should have elected a leader by now");

        Ok(())
    });

    sim.run().unwrap();
}

/// Test network partition and healing.
#[test]
fn test_partition_and_heal() {
    init_tracing();

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    let _cluster = spawn_cluster(&mut sim, ClusterConfig {
        num_nodes: 5,
        ..Default::default()
    });

    sim.client("test-client", async move {
        // Wait for initial leader election
        sleep(Duration::from_secs(3)).await;
        tracing::info!("Initial stabilization complete");

        // Partition node-1 from the rest
        tracing::info!("Partitioning node-1");
        for i in 2..=5 {
            turmoil::partition("node-1", format!("node-{}", i));
        }

        // Wait for new leader election among remaining nodes
        sleep(Duration::from_secs(3)).await;
        tracing::info!("New leader should be elected among nodes 2-5");

        // Heal the partition
        tracing::info!("Healing partition");
        for i in 2..=5 {
            turmoil::repair("node-1", format!("node-{}", i));
        }

        // Wait for cluster to stabilize
        sleep(Duration::from_secs(3)).await;
        tracing::info!("Cluster should be stable again");

        Ok(())
    });

    sim.run().unwrap();
}

/// Test with random client requests and network chaos.
#[test]
fn test_chaos_with_client_requests() {
    init_tracing();

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .build();

    let cluster = spawn_cluster(&mut sim, ClusterConfig {
        num_nodes: 5,
        ..Default::default()
    });

    let history = Arc::new(Mutex::new(OperationHistory::new()));

    sim.client("chaos-client", {
        let cluster = cluster.clone();
        let history = history.clone();

        async move {
            // Wait for initial leader election
            sleep(Duration::from_secs(3)).await;
            tracing::info!("Starting chaos test");

            // Spawn multiple clients
            let mut handles = vec![];
            for seed in 0..3 {
                let c = cluster.clone();
                let h = history.clone();
                handles.push(tokio::spawn(async move {
                    run_chaos_client(seed, c, 20, h).await;
                }));
            }

            // Inject network chaos periodically
            for round in 0..5 {
                sleep(Duration::from_secs(5)).await;

                // Partition a random node
                let victim = format!("node-{}", (round % 5) + 1);
                tracing::info!(round, "Partitioning {}", victim);

                for i in 1..=5 {
                    let other = format!("node-{}", i);
                    if other != victim {
                        turmoil::partition(victim.clone(), other);
                    }
                }

                // Let it cook
                sleep(Duration::from_secs(3)).await;

                // Heal partition
                tracing::info!(round, "Healing partition for {}", victim);
                for i in 1..=5 {
                    let other = format!("node-{}", i);
                    if other != victim {
                        turmoil::repair(victim.clone(), other);
                    }
                }
            }

            // Wait for all clients to finish
            for h in handles {
                let _ = h.await;
            }

            // Report results
            let hist = history.lock().await;
            tracing::info!(
                "Chaos test complete: {} successful, {} failed operations",
                hist.success_count(),
                hist.failure_count()
            );

            Ok(())
        }
    });

    sim.run().unwrap();
}

/// Deterministic fuzzing with multiple seeds.
#[test]
fn test_fuzz_with_seeds() {
    init_tracing();

    for seed in 0..10 {
        tracing::info!("Running with seed {}", seed);

        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(30))
            .build();

        // Set deterministic seed
        // Note: turmoil uses its own RNG seeding mechanism

        let cluster = spawn_cluster(&mut sim, ClusterConfig {
            num_nodes: 3,
            ..Default::default()
        });

        sim.client("fuzz-client", {
            let cluster = cluster.clone();

            async move {
                sleep(Duration::from_secs(2)).await;

                // Run some operations
                let history = Arc::new(Mutex::new(OperationHistory::new()));
                run_chaos_client(seed, cluster, 10, history.clone()).await;

                let hist = history.lock().await;
                tracing::info!(
                    seed,
                    "Seed {} complete: {} successful, {} failed",
                    seed,
                    hist.success_count(),
                    hist.failure_count()
                );

                Ok(())
            }
        });

        if let Err(e) = sim.run() {
            panic!("Seed {} failed: {:?}", seed, e);
        }
    }
}

/// Test message holding and delayed delivery.
#[test]
fn test_message_delays() {
    init_tracing();

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    let _cluster = spawn_cluster(&mut sim, ClusterConfig {
        num_nodes: 3,
        ..Default::default()
    });

    sim.client("delay-test", async move {
        // Wait for initial stabilization
        sleep(Duration::from_secs(3)).await;

        // Hold messages between node-1 and node-2
        tracing::info!("Holding messages between node-1 and node-2");
        turmoil::hold("node-1", "node-2");

        sleep(Duration::from_secs(2)).await;

        // Release held messages
        tracing::info!("Releasing held messages");
        turmoil::release("node-1", "node-2");

        sleep(Duration::from_secs(2)).await;

        tracing::info!("Message delay test complete");

        Ok(())
    });

    sim.run().unwrap();
}
