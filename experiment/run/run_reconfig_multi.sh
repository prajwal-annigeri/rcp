#!/usr/bin/env bash

set -euo pipefail

N_RUNS=""
RUNNING_TIME=""

usage() {
  cat <<USAGE
Usage: $0 -n <runs> --time <seconds>

Runs reconfiguration experiments for all three modes (joint, recraft, orca)
using default flags from run_reconfig.sh, repeated n times per mode.

Options:
  -n <runs>         Number of repetitions per protocol mode (required)
  --time <seconds>  Experiment duration passed to run_reconfig.sh (required)
  -h, --help        Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n)
      N_RUNS="${2:-}"
      shift 2
      ;;
    --time)
      RUNNING_TIME="${2:-}"
      shift 2
      ;;
    --time=*)
      RUNNING_TIME="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$N_RUNS" || -z "$RUNNING_TIME" ]]; then
  echo "Both -n and --time are required."
  usage
  exit 1
fi

if ! [[ "$N_RUNS" =~ ^[0-9]+$ ]] || (( N_RUNS <= 0 )); then
  echo "Invalid -n=$N_RUNS. Must be a positive integer."
  exit 1
fi

if ! [[ "$RUNNING_TIME" =~ ^[0-9]+$ ]] || (( RUNNING_TIME <= 0 )); then
  echo "Invalid --time=$RUNNING_TIME. Must be a positive integer (seconds)."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run_reconfig.sh"
OUTPUT_DIR="$SCRIPT_DIR/../output"

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "run_reconfig.sh not found or not executable: $RUN_SCRIPT"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

MODES=(recraft)

echo "Starting multi-run reconfiguration experiments"
echo "Runs per mode: $N_RUNS"
echo "Time per run:  ${RUNNING_TIME}s"
echo "Modes:         ${MODES[*]}"
echo ""

for mode in "${MODES[@]}"; do
  for ((run=1; run<=N_RUNS; run++)); do
    echo "=== Mode=$mode Run=$run/$N_RUNS ==="

    # run_reconfig.sh prompts for ENTER once before starting.
    printf '\n' | bash "$RUN_SCRIPT" --reconfig-mode "$mode" --time "$RUNNING_TIME"

    # Preserve each run output so subsequent runs don't overwrite it.
    latest_file="$(ls -t "$OUTPUT_DIR"/reconfig-protocol=raft-mode=${mode}-N=*-interval=*-time=${RUNNING_TIME}.txt 2>/dev/null | head -n 1 || true)"
    if [[ -n "$latest_file" && -f "$latest_file" ]]; then
      base="${latest_file%.txt}"
      archived="${base}-run-${run}.txt"
      cp "$latest_file" "$archived"
      echo "Saved run artifact: $archived"
    else
      echo "Warning: Could not find output artifact for mode=$mode run=$run"
    fi

    echo ""
  done
done

echo "All runs completed."
