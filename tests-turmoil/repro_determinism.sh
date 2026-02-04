#!/bin/bash
set -e

# Configuration
STEPS=1000
RUNS=10
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
    
    # Run the fuzzer (using local cargo if available, otherwise docker)
    if command -v cargo >/dev/null 2>&1; then
        RUST_LOG=info cargo run --bin fuzz -- --iterations 1 --steps "$STEPS" --seed "$SEED" > "$LOG_DIR/run_$i.log" 2>&1
    else
        docker run --rm -v "$(pwd)/..:/app" -w /app/tests-turmoil rust:1.92-bookworm /bin/bash -c "RUST_LOG=info cargo run --bin fuzz -- --iterations 1 --steps $STEPS --seed $SEED" > "$LOG_DIR/run_$i.log" 2>&1
    fi
    
    # Normalize the logs
    # 1. Extract simulation lines
    # 2. Mask embedded durations and clock-based metadata
    # 3. Mask HH:MM:SS.ffffff timestamps
    sed -n '/Starting simulation/,$p' "$LOG_DIR/run_$i.log" | 
    sed -E 's/tv_sec: [0-9]+, tv_nsec: [0-9]+/tv_sec: MASKED, tv_nsec: MASKED/g' | 
    sed -E 's/[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}/HH:MM:SS.ffffff/g' | 
    sed -E 's/@[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}/@HH:MM:SS.ffffff/g' | 
    sed -E 's/[0-9]+ms ago/Xms ago/g' | 
    sed -E 's/[0-9]+\.[0-9]{3}s/X.XXXs/g' > "$LOG_DIR/run_$i.sim"
    
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
