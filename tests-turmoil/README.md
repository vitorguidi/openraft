# OpenRaft Turmoil Simulation Tests

This package provides a deterministic simulation environment for OpenRaft using the [Turmoil](https://github.com/tokio-rs/turmoil) framework. It is designed to detect deep protocol violations and edge cases by simulating complex network conditions like partitions, packet loss, and message delays in a controlled, repeatable manner.

## Deterministic Fuzzer

The core of this testing suite is a tick-based fuzzer located in `src/bin/fuzz.rs`. Unlike traditional integration tests that rely on real time and asynchronous events, this fuzzer controls the progression of time explicitly.

### Architecture

The fuzzer operates in a strict loop:
1. **`sim.step()`**: Advances the simulation by a single discrete "tick".
2. **Invariant Checking**: Immediately after the step, the fuzzer pulls the internal state of every node in the cluster and verifies cluster-wide invariants.

This "stop-the-world" approach ensures that even transient invariant violations—which might be missed in a concurrent environment—are captured the moment they occur.

## Synchronous State Snapshot API

To support high-frequency invariant checking without interfering with the simulation's determinism or introducing asynchronous overhead, a synchronous state API was added to the `openraft` core.

### `Raft::state_snapshot()`

The `Raft` node now exposes a `state_snapshot()` method. This method returns a `RaftStateSnapshot` struct containing:
* **Vote/Term**: Current term and voting status.
* **Log State**: The complete list of log IDs and purged log metadata.
* **Membership**: Current effective and committed membership configurations.
* **Server State**: The node's role (Leader, Follower, Candidate, etc.).
* **IO Progress**: Exactly where the node is in terms of accepted, applied, and committed logs.

### Implementation Details
* **Watch Channel**: Internal state changes in `RaftCore` are pushed to a synchronous `watch` channel.
* **Zero Async**: Accessing the snapshot is a non-blocking, synchronous operation that simply clones the latest value from the channel.
* **Continuous Updates**: Snapshots are updated in the `report_metrics` loop of `RaftCore`, ensuring the fuzzer always sees the most recent stable state.

## Determinism Enforcement

To achieve bit-for-bit reproducible simulations, the following mechanisms were implemented:

### 1. Deterministic RNG Shim
OpenRaft normally relies on the system's global random pool for election timeouts, which is non-deterministic. We introduced a `task_local!` deterministic RNG in `openraft-rt-tokio`. 
* Each Raft node is assigned a private, seeded `SmallRng` instance.
* When OpenRaft core requests a random number via the `AsyncRuntime` trait, it pulls from this scoped source instead of the system's global pool.

### 2. Node Seeding
In `spawn_cluster`, every node is initialized with a deterministic seed derived from the simulation's root seed (`node_seed = root_seed + node_id`). This ensures that even across multiple nodes, the sequence of "random" election timeouts is predictable and repeatable.

### 3. Library Version Alignment
Turmoil depends on `rand 0.8`, while the main OpenRaft project uses `rand 0.9`. To prevent version clashing, `tests-turmoil` explicitly aliases these versions:
* **`rand` (0.8)**: Used for the Turmoil simulation engine and network chaos logic.
* **`rand_09` (0.9)**: Used for OpenRaft's internal logic and the deterministic RNG shim.

### 4. Committed-State Invariants
To avoid "False Positives" in log consistency checks, the fuzzer only validates log entries that have been **committed** by both nodes. This recognizes that uncommitted entries can naturally diverge and be overwritten during standard Raft leader transitions.

### 6. Verifying Determinism
You can verify that the simulation is bit-for-bit deterministic by running the reproduction script:

```bash
cd tests-turmoil
./repro_determinism.sh
```

This script runs the fuzzer 10 times with the same seed, normalizes the logs (by masking timestamps), and verifies that the resulting execution traces are identical using SHA256 hashes.

## Running the Fuzzer

### Prerequisites
The codebase uses modern Rust features like "let chains" which require **Rust 1.92+** or a recent nightly.

### Execution
You can run the fuzzer directly with `cargo`:

```bash
cd tests-turmoil
cargo run --bin fuzz -- --iterations 1 --steps 10000
```

### Using Docker
If your local environment has an older Rust version, use the provided Docker strategy:

```bash
docker run --rm -v "$(pwd):/app" -w /app rust:1.92-bookworm /bin/bash -c "cd tests-turmoil && cargo run --bin fuzz"
```

### Options
* `-s, --seed <SEED>`: Provide a specific seed to reproduce a failure.
* `-i, --iterations <N>`: Number of independent simulation runs (0 for infinite).
* `-n, --nodes <N>`: Cluster size (default: 5).
* `-f, --fail-rate <RATE>`: Probability of message failure (0.0 to 1.0).
* `--steps <N>`: Maximum number of ticks per iteration.
* `--no-chaos`: Disable network partition and hold injection.
