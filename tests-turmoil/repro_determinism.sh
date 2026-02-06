#!/bin/bash
set -e

# Configuration
STEPS=10000
RUNS=5
SEED=12345
LOG_DIR="repro_logs"

echo "=== Reproducing Determinism Enforcement ==="
echo "Steps per run: $STEPS"
echo "Total runs: $RUNS"
echo "Seed: $SEED"
echo ""

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.sim

for i in $(seq 1 $RUNS); do
    echo -n "Run $i/$RUNS... "
    
    # Run the fuzzer using docker
    docker run --rm -v "$(pwd)/..:/app" -w /app/tests-turmoil rust:1.92-bookworm /bin/bash -c "RUST_LOG=info cargo run --bin fuzz -- --iterations 1 --steps $STEPS --seed $SEED" > "$LOG_DIR/run_$i.log" 2>&1
    
    # Normalize the logs
    # Extract from 'Iteration' to 'Final Results'
    sed -n '/Iteration 1/,/Final Results/p' "$LOG_DIR/run_$i.log" | \
    sed -E 's/tv_sec: [0-9]+, tv_nsec: [0-9]+/tv_sec: MASKED, tv_nsec: MASKED/g' | \
    sed -E 's/[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}/HH:MM:SS.ffffff/g' | \
    sed -E 's/@[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}/@HH:MM:SS.ffffff/g' | \
    sed -E 's/[0-9]+ms ago/Xms ago/g' | \
    sed -E 's/[0-9]+\.[0-9]{3}s/X.XXXs/g' > "$LOG_DIR/run_$i.sim"
    
    if [ ! -s "$LOG_DIR/run_$i.sim" ]; then
        echo "ERROR: Simulation log is empty. Check $LOG_DIR/run_$i.log"
        exit 1
    fi

    echo "Hash: $(sha256sum "$LOG_DIR/run_$i.sim" | cut -d' ' -f1)"
done

echo ""
UNIQUE_HASHES=$(sha256sum "$LOG_DIR"/*.sim | cut -d' ' -f1 | sort | uniq | wc -l)

if [ "$UNIQUE_HASHES" -eq 1 ]; then
    echo "SUCCESS: All $RUNS runs produced identical deterministic traces."
    # Clean up
    rm -rf "$LOG_DIR"
else
    echo "FAILURE: Detected $UNIQUE_HASHES different execution traces. Determinism is broken."
    exit 1
fi
