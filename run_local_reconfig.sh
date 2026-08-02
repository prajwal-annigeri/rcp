#!/usr/bin/env bash

set -euo pipefail

APP_EXEC="main.go"
PROTOCOL="raft"
RECONFIG_MODE="joint" # joint|recraft|orca

BATCH_LOW=256
BATCH_HIGH=512
BACKOFF_DEC=100
TIMEOUT_CONSENSUS=300
TIMEOUT_ELECTION_MIN=150
TIMEOUT_ELECTION_MAX=300
TIMEOUT_BATCH=4
TIMEOUT_HEARTBEAT=50

RUNNING_TIME=30
STARTUP_WAIT=3
RECONFIG_INTERVAL=2
PATTERN="A|B|C,A|B|C|D|E|F|G"
PATTERN_TARGETS=()
RETRIES=100
RETRY_DELAY_MS=10
RECONFIG_CLIENT_GRACE=5

CONFIG_FILE="./nodes.json"
OUTPUT_ROOT="./output/local_reconfig"

OUT_DIR=""
CLEANED_UP=0
SERVER_BIN=""
RECONFIG_CLIENT_BIN=""
TMUX_SESSION=""

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --reconfig-mode <joint|recraft|orca>
  --time <seconds>
  --startup-wait <seconds>
  --reconfig-interval <seconds>
  --pattern <csv_sets>
  --retries <count>
  --retry-delay-ms <milliseconds>
  --client-grace <seconds>
  --config-file <path>
  --output-root <path>
  --tmux-session <name>
  --batch-low <n>
  --batch-high <n>
  --backoff-dec <n>
  --ct <milliseconds>
  --et-min <milliseconds>
  --et-max <milliseconds>
  --bt <milliseconds>
  --ht <milliseconds>
USAGE
}

tmux_session_exists() {
  local session="$1"
  tmux has-session -t "$session" >/dev/null 2>&1
}

tmux_window_exists() {
  local session="$1"
  local window="$2"
  tmux list-windows -t "$session" -F '#W' 2>/dev/null | grep -qx "$window"
}

duration_to_us() {
  local d="$1"
  awk -v d="$d" '
    BEGIN {
      if (d ~ /µs$/ || d ~ /us$/) {
        gsub(/µs$/, "", d); gsub(/us$/, "", d); printf "%.6f\n", d + 0; exit
      }
      if (d ~ /ms$/) {
        gsub(/ms$/, "", d); printf "%.6f\n", (d + 0) * 1000.0; exit
      }
      if (d ~ /ns$/) {
        gsub(/ns$/, "", d); printf "%.6f\n", (d + 0) / 1000.0; exit
      }
      if (d ~ /s$/) {
        gsub(/s$/, "", d); printf "%.6f\n", (d + 0) * 1000000.0; exit
      }
      print "-1"
    }
  '
}

us_to_ms() {
  local us="$1"
  awk -v us="$us" 'BEGIN { printf "%.3fms", us / 1000.0 }'
}

node_in_voter_list() {
  local node_id="$1"
  local voters="$2"
  local v
  for v in $voters; do
    if [[ "$v" == "$node_id" ]]; then
      return 0
    fi
  done
  return 1
}

lookup_voters_for_time() {
  local target_time="$1"
  local set_count="${#PATTERN_TARGETS[@]}"
  if (( set_count == 0 || RECONFIG_INTERVAL <= 0 || target_time <= 0 || target_time % RECONFIG_INTERVAL != 0 )); then
    return 1
  fi
  local idx=$(( (target_time / RECONFIG_INTERVAL - 1) % set_count ))
  echo "${PATTERN_TARGETS[$idx]//|/ }"
}

append_violation_report() {
  local reconfig_log="$1"
  local source_log="${reconfig_log}.violation_source"
  local success_count=0
  local violation_count=0
  local have_prev=0
  local prev_voters=""
  local line

  cp "$reconfig_log" "$source_log"

  {
    echo "=== Reconfiguration Violation Report ==="
  } >>"$reconfig_log"

  while IFS= read -r line; do
    if [[ "$line" =~ Reconfiguration\ at\ t=([0-9]+)\ succeeded\ via\ ([^[:space:]]+) ]]; then
      local t="${BASH_REMATCH[1]}"
      local via="${BASH_REMATCH[2]}"
      local voters
      voters="$(lookup_voters_for_time "$t" || true)"

      if [[ -z "$voters" ]]; then
        echo "t=${t}s status=SKIP via=${via} reason=missing target voter set mapping" >>"$reconfig_log"
        continue
      fi

      success_count=$((success_count + 1))

      if [[ "$have_prev" -eq 0 ]]; then
        echo "t=${t}s status=BASELINE via=${via} voters=[${voters}] (no previous successful reconfiguration)" >>"$reconfig_log"
        prev_voters="$voters"
        have_prev=1
        continue
      fi

      if node_in_voter_list "$via" "$prev_voters"; then
        echo "t=${t}s status=OK via=${via} voters=[${voters}]" >>"$reconfig_log"
      else
        violation_count=$((violation_count + 1))
        echo "t=${t}s status=VIOLATION via=${via} voters=[${voters}] last_success_voters=[${prev_voters}]" >>"$reconfig_log"
      fi

      prev_voters="$voters"
    fi
  done <"$source_log"

  rm -f "$source_log"

  {
    echo "=== Reconfiguration Violation Summary ==="
    echo "successful=${success_count} violations=${violation_count}"
    if [[ "$success_count" -eq 0 ]]; then
      echo "No successful reconfigurations to evaluate."
    fi
  } >>"$reconfig_log"
}

append_latency_summary_report() {
  local reconfig_log="$1"
  local samples_file="${reconfig_log}.latency_samples"
  : >"$samples_file"

  while IFS= read -r line; do
    if [[ "$line" =~ succeeded\ via\ [^[:space:]]+\ in\ ([^[:space:]]+)\ \(active=([^[:space:]]+)\ retry-delay=([^[:space:]]+)\) ]]; then
      total_us="$(duration_to_us "${BASH_REMATCH[1]}")"
      active_us="$(duration_to_us "${BASH_REMATCH[2]}")"
      retry_us="$(duration_to_us "${BASH_REMATCH[3]}")"
      if [[ "$total_us" != "-1" && "$active_us" != "-1" && "$retry_us" != "-1" ]]; then
        echo "$total_us $active_us $retry_us" >>"$samples_file"
      fi
    fi
  done <"$reconfig_log"

  local success_count
  success_count=$(wc -l <"$samples_file" | tr -d ' ')

  {
    echo "=== Script Latency Summary ==="
    echo "successful=${success_count}"
  } >>"$reconfig_log"

  if [[ "$success_count" -eq 0 ]]; then
    echo "No successful reconfigurations to summarize." >>"$reconfig_log"
    rm -f "$samples_file"
    return
  fi

  local sorted_total="${samples_file}.total.sorted"
  local sorted_active="${samples_file}.active.sorted"
  local sorted_retry="${samples_file}.retry.sorted"

  awk '{print $1}' "$samples_file" | sort -n >"$sorted_total"
  awk '{print $2}' "$samples_file" | sort -n >"$sorted_active"
  awk '{print $3}' "$samples_file" | sort -n >"$sorted_retry"

  percentile_rank() {
    local p="$1"
    local n="$2"
    awk -v p="$p" -v n="$n" 'BEGIN { r = int((p/100.0)*n); if ((p/100.0)*n > r) r++; if (r < 1) r = 1; if (r > n) r = n; print r }'
  }

  read_nth() {
    local file="$1"
    local n="$2"
    awk -v n="$n" 'NR==n { print; exit }' "$file"
  }

  summarize_metric() {
    local name="$1"
    local file="$2"
    local n="$3"
    local p50_rank p90_rank p95_rank p99_rank min max avg p50 p90 p95 p99
    p50_rank=$(percentile_rank 50 "$n")
    p90_rank=$(percentile_rank 90 "$n")
    p95_rank=$(percentile_rank 95 "$n")
    p99_rank=$(percentile_rank 99 "$n")
    min=$(head -n 1 "$file")
    max=$(tail -n 1 "$file")
    avg=$(awk '{s+=$1} END { if (NR==0) print 0; else printf "%.6f\n", s/NR }' "$file")
    p50=$(read_nth "$file" "$p50_rank")
    p90=$(read_nth "$file" "$p90_rank")
    p95=$(read_nth "$file" "$p95_rank")
    p99=$(read_nth "$file" "$p99_rank")
    echo "${name}: min=$(us_to_ms "$min") max=$(us_to_ms "$max") avg=$(us_to_ms "$avg") p50=$(us_to_ms "$p50") p90=$(us_to_ms "$p90") p95=$(us_to_ms "$p95") p99=$(us_to_ms "$p99")" >>"$reconfig_log"
  }

  summarize_metric "total_latency" "$sorted_total" "$success_count"
  summarize_metric "active_latency" "$sorted_active" "$success_count"
  summarize_metric "retry_delay" "$sorted_retry" "$success_count"

  rm -f "$samples_file" "$sorted_total" "$sorted_active" "$sorted_retry"
}

cleanup() {
  if [[ "$CLEANED_UP" -eq 1 ]]; then
    return
  fi
  CLEANED_UP=1

  set +e
  if [[ -n "$TMUX_SESSION" ]] && tmux_session_exists "$TMUX_SESSION"; then
    tmux kill-session -t "$TMUX_SESSION" >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reconfig-mode)
      RECONFIG_MODE="$2"
      shift 2
      ;;
    --time)
      RUNNING_TIME="$2"
      shift 2
      ;;
    --startup-wait)
      STARTUP_WAIT="$2"
      shift 2
      ;;
    --reconfig-interval)
      RECONFIG_INTERVAL="$2"
      shift 2
      ;;
    --pattern)
      PATTERN="$2"
      shift 2
      ;;
    --retries)
      RETRIES="$2"
      shift 2
      ;;
    --retry-delay-ms)
      RETRY_DELAY_MS="$2"
      shift 2
      ;;
    --client-grace)
      RECONFIG_CLIENT_GRACE="$2"
      shift 2
      ;;
    --config-file)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --tmux-session)
      TMUX_SESSION="$2"
      shift 2
      ;;
    --batch-low)
      BATCH_LOW="$2"
      shift 2
      ;;
    --batch-high)
      BATCH_HIGH="$2"
      shift 2
      ;;
    --backoff-dec)
      BACKOFF_DEC="$2"
      shift 2
      ;;
    --ct)
      TIMEOUT_CONSENSUS="$2"
      shift 2
      ;;
    --et-min)
      TIMEOUT_ELECTION_MIN="$2"
      shift 2
      ;;
    --et-max)
      TIMEOUT_ELECTION_MAX="$2"
      shift 2
      ;;
    --bt)
      TIMEOUT_BATCH="$2"
      shift 2
      ;;
    --ht)
      TIMEOUT_HEARTBEAT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ "$RECONFIG_MODE" != "joint" && "$RECONFIG_MODE" != "recraft" && "$RECONFIG_MODE" != "orca" ]]; then
  echo "Invalid --reconfig-mode=$RECONFIG_MODE. Expected: joint|recraft|orca"
  exit 1
fi

for number in "$RUNNING_TIME" "$STARTUP_WAIT" "$RECONFIG_INTERVAL" "$RETRIES" "$RETRY_DELAY_MS" "$RECONFIG_CLIENT_GRACE"; do
  if ! [[ "$number" =~ ^[0-9]+$ ]]; then
    echo "Expected non-negative integer but got: $number"
    exit 1
  fi
done

if [[ "$RUNNING_TIME" -le 0 || "$RECONFIG_INTERVAL" -le 0 || "$RETRIES" -le 0 ]]; then
  echo "--time, --reconfig-interval, and --retries must be > 0"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go is required"
  exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE"
  exit 1
fi

NODE_IDS=()
while IFS= read -r node_id; do
  NODE_IDS+=("$node_id")
done < <(jq -r '.nodes[].id' "$CONFIG_FILE")

if [[ "${#NODE_IDS[@]}" -eq 0 ]]; then
  echo "No node IDs found in $CONFIG_FILE"
  exit 1
fi

IFS=',' read -r -a PATTERNS <<< "$PATTERN"
if [[ "${#PATTERNS[@]}" -eq 0 ]]; then
  echo "Invalid --pattern. Expected comma-separated voter sets."
  exit 1
fi

PATTERN_TARGETS=()
for raw_set in "${PATTERNS[@]}"; do
  trimmed="${raw_set#"${raw_set%%[![:space:]]*}"}"
  trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
  if [[ -z "$trimmed" ]]; then
    continue
  fi
  PATTERN_TARGETS+=("$trimmed")
done

if [[ "${#PATTERN_TARGETS[@]}" -eq 0 ]]; then
  echo "Invalid --pattern. No valid voter sets found."
  exit 1
fi

if (( RUNNING_TIME < RECONFIG_INTERVAL )); then
  echo "No reconfiguration events generated. Ensure --time >= --reconfig-interval."
  exit 1
fi

RECONFIG_EVENT_COUNT=$((RUNNING_TIME / RECONFIG_INTERVAL))

OUT_DIR="${OUTPUT_ROOT}"
mkdir -p "$OUT_DIR"

# Reuse a stable output directory for each run.
rm -f \
  "${OUT_DIR}"/server_*.log \
  "${OUT_DIR}"/reconfig_client.log \
  "${OUT_DIR}"/combined.log \
  "${OUT_DIR}"/run_meta.txt \
  "${OUT_DIR}"/run_server_*.sh \
  "${OUT_DIR}"/run_reconfig_client.sh \
  "${OUT_DIR}"/rcp_server \
  "${OUT_DIR}"/reconfig_client_bin

if [[ -z "$TMUX_SESSION" ]]; then
  TMUX_SESSION="local_reconfig"
fi

trap cleanup EXIT INT TERM

echo "Starting local reconfiguration run"
echo "Mode:                $RECONFIG_MODE"
echo "Config:              $CONFIG_FILE"
echo "Nodes:               ${NODE_IDS[*]}"
echo "Experiment Time:     ${RUNNING_TIME}s"
echo "Startup Wait:        ${STARTUP_WAIT}s"
echo "Reconfig Interval:   ${RECONFIG_INTERVAL}s"
echo "Pattern:             $PATTERN"
echo "Scheduled Events:    $RECONFIG_EVENT_COUNT"
echo "Tmux Session:        $TMUX_SESSION"
echo "Output Folder:       $OUT_DIR"
echo ""

SERVER_BIN="${OUT_DIR}/rcp_server"
RECONFIG_CLIENT_BIN="${OUT_DIR}/reconfig_client_bin"

go build -o "$SERVER_BIN" "$APP_EXEC"
go build -o "$RECONFIG_CLIENT_BIN" ./reconfig_client/main.go

if tmux_session_exists "$TMUX_SESSION"; then
  tmux kill-session -t "$TMUX_SESSION" >/dev/null 2>&1 || true
fi

first_window=1
for node_id in "${NODE_IDS[@]}"; do
  log_file="${OUT_DIR}/server_${node_id}.log"
  runner="${OUT_DIR}/run_server_${node_id}.sh"
  cat >"$runner" <<RUNNER
#!/usr/bin/env bash
set -euo pipefail
exec "$SERVER_BIN" \
  --id "$node_id" \
  --logs \
  --protocol "$PROTOCOL" \
  --reconfig-mode "$RECONFIG_MODE" \
  --config-file "$CONFIG_FILE" \
  --batch-low "$BATCH_LOW" \
  --batch-high "$BATCH_HIGH" \
  --backoff-decrement "$BACKOFF_DEC" \
  --ct "$TIMEOUT_CONSENSUS" \
  --et-min "$TIMEOUT_ELECTION_MIN" \
  --et-max "$TIMEOUT_ELECTION_MAX" \
  --bt "$TIMEOUT_BATCH" \
  --ht "$TIMEOUT_HEARTBEAT" \
  >>"$log_file" 2>&1
RUNNER
  chmod +x "$runner"

  if [[ "$first_window" -eq 1 ]]; then
    tmux new-session -d -s "$TMUX_SESSION" -n "srv_${node_id}" "$runner"
    first_window=0
  else
    tmux new-window -d -t "$TMUX_SESSION:" -n "srv_${node_id}" "$runner"
  fi
  echo "Started server ${node_id} in tmux window srv_${node_id}"
done

sleep "$STARTUP_WAIT"

RECONFIG_LOG="${OUT_DIR}/reconfig_client.log"
client_runner="${OUT_DIR}/run_reconfig_client.sh"
cat >"$client_runner" <<RUNNER
#!/usr/bin/env bash
set -euo pipefail
exec "$RECONFIG_CLIENT_BIN" \
  --config-file "$CONFIG_FILE" \
  --pattern "$PATTERN" \
  --reconfig-interval "$RECONFIG_INTERVAL" \
  --time "$RUNNING_TIME" \
  --retries "$RETRIES" \
  --retry-delay-ms "$RETRY_DELAY_MS" \
  >>"$RECONFIG_LOG" 2>&1
RUNNER
chmod +x "$client_runner"

tmux new-window -d -t "$TMUX_SESSION:" -n "reconfig_client" "$client_runner"
echo "Started reconfig client in tmux window reconfig_client"

sleep "$RUNNING_TIME"

if tmux_window_exists "$TMUX_SESSION" "reconfig_client"; then
  echo "Waiting up to ${RECONFIG_CLIENT_GRACE}s for reconfig client to finish..."
  wait_deadline=$((SECONDS + RECONFIG_CLIENT_GRACE))
  while tmux_window_exists "$TMUX_SESSION" "reconfig_client" && (( SECONDS < wait_deadline )); do
    sleep 1
  done
fi

timed_out=1
client_status=0

echo "Experiment time reached (${RUNNING_TIME}s). Stopping tmux session."
cleanup

append_violation_report "$RECONFIG_LOG"
append_latency_summary_report "$RECONFIG_LOG"

COMBINED_LOG="${OUT_DIR}/combined.log"
: > "$COMBINED_LOG"

for file in "${OUT_DIR}"/server_*.log "${OUT_DIR}/reconfig_client.log"; do
  if [[ -f "$file" ]]; then
    base="$(basename "$file")"
    sed "s/^/[${base}] /" "$file" >> "$COMBINED_LOG"
  fi
done

{
  echo "mode=$RECONFIG_MODE"
  echo "config_file=$CONFIG_FILE"
  echo "nodes=${NODE_IDS[*]}"
  echo "running_time=$RUNNING_TIME"
  echo "startup_wait=$STARTUP_WAIT"
  echo "reconfig_interval=$RECONFIG_INTERVAL"
  echo "pattern=$PATTERN"
  echo "scheduled_events=$RECONFIG_EVENT_COUNT"
  echo "retries=$RETRIES"
  echo "retry_delay_ms=$RETRY_DELAY_MS"
  echo "tmux_session=$TMUX_SESSION"
  echo "timed_out=$timed_out"
  echo "client_status=$client_status"
} > "${OUT_DIR}/run_meta.txt"

echo "Done. Logs are in ${OUT_DIR}"
echo "Combined log: ${COMBINED_LOG}"
echo "Latency summary:"
tail -n 8 "$RECONFIG_LOG"

exit 0
