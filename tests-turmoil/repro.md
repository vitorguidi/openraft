# Reproduction Instructions for LeaderMissingCommitted Bug

This document describes how to reproduce the `LeaderMissingCommitted` violation encountered during detsim testing.

## 1. Build the Buggy Docker Image

First, ensure you are on the buggified commit and build the Docker image:

```bash
cd ~/projects/openraft
git checkout eb05b62f54c8a463a91c9dafa80ff447059ae439
docker build -t openraft-detsim:buggy_leader -f tests-turmoil/Dockerfile .
```

## 2. Create the Crash File

Save the following JSON content to a file named `crash.json` in your current directory:

```json
{
  "base_seed": 1770363389478012250,
  "config": {
    "chaos_interval": 3112,
    "election_timeout_max": 703,
    "election_timeout_min": 234,
    "enable_chaos": true,
    "fail_rate": 0.0,
    "heartbeat_interval": 78,
    "max_potential_nodes": 10,
    "membership_interval": 23825,
    "num_initial_nodes": 3,
    "restart_chance": 0.04181629574769935
  },
  "iteration": 0,
  "iteration_seed": 1770363389478012250,
  "max_steps": 10000000000,
  "reproduce": {
    "command": "cargo run --bin fuzz -- --reproduce 1770363389478012250 --max-steps 10000000000 --crash-file /tmp/crash-7a1411c5-2a68-4ccc-9729-b0833d10b844.json",
    "iteration_seed": 1770363389478012250,
    "max_steps": 10000000000
  },
  "steps_completed": 20827,
  "violation": "Step 20827: LeaderMissingCommitted { term: 2, leader: 7, missing_index: 125 }"
}
```

## 3. Run Reproduction with Docker

Execute the simulation in the container using the reproduction seed. We redirect output to a file to prevent terminal flooding and potential crashes in the CLI environment:

```bash
docker run --rm 
  --entrypoint /usr/local/bin/fuzz 
  openraft-detsim:buggy_leader 
  --reproduce 1770363389478012250 
  --max-steps 10000000000 
  > repro.log 2>&1
```

## 4. Confirm Reproduction

Once the command finishes, check the end of `repro.log` for the violation. It should match the crash reported:

```bash
grep "VIOLATION" repro.log
```

**Expected Output:**
`VIOLATION: Step 20827: LeaderMissingCommitted { term: 2, leader: 7, missing_index: 125 }`
