#!/bin/bash

KEY="./../key"
APP_EXEC="app"
RECONFIG_EXEC="reconfig-client"
USER="ec2-user"

# Default values
PROTOCOL="raft"
RECONFIG_MODE="joint" # joint|recraft|orca
BATCH_LOW=1
BATCH_HIGH=512
BACKOFF_DEC=100
TIMEOUT_CONSENSUS=2400
TIMEOUT_ELECTION_MIN=1200
TIMEOUT_ELECTION_MAX=2400
TIMEOUT_BATCH=0
TIMEOUT_HEARTBEAT=300
LOGGING="false"

RUNNING_TIME=30
RECONFIG_INTERVAL=1
PATTERN="A|C|E,A|B|C|D|E|F|G"
RETRIES=100
RETRY_DELAY_MS=10
RECONFIGURE_TIMEOUT_MS=1500
RECONFIG_CLIENT_GRACE=5
TIME_WAS_SET="false"

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

append_script_latency_summary() {
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --time=*)
      RUNNING_TIME="${1#*=}"
      TIME_WAS_SET="true"
      shift
      ;;
    --reconfig-mode)
      RECONFIG_MODE="$2"
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
    --logging)
      LOGGING="$2"
      shift 2
      ;;
    --time)
      if [[ -z "$2" || "$2" == --* ]]; then
        echo "Missing value for --time"
        exit 1
      fi
      RUNNING_TIME="$2"
      TIME_WAS_SET="true"
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
    --reconfigure-timeout-ms)
      RECONFIGURE_TIMEOUT_MS="$2"
      shift 2
      ;;
    --client-grace)
      RECONFIG_CLIENT_GRACE="$2"
      shift 2
      ;;
    *)
      echo "Unknown or malformed option: $1"
      exit 1
      ;;
  esac
done

if [[ "$RECONFIG_MODE" != "joint" && "$RECONFIG_MODE" != "recraft" && "$RECONFIG_MODE" != "orca" ]]; then
  echo "Invalid --reconfig-mode=$RECONFIG_MODE. Expected: joint|recraft|orca"
  exit 1
fi

if ! [[ "$RUNNING_TIME" =~ ^[0-9]+$ ]] || [ "$RUNNING_TIME" -le 0 ]; then
  echo "Invalid --time=$RUNNING_TIME. Must be positive integer seconds."
  exit 1
fi

if ! [[ "$RECONFIG_INTERVAL" =~ ^[0-9]+$ ]] || [ "$RECONFIG_INTERVAL" -le 0 ]; then
  echo "Invalid --reconfig-interval=$RECONFIG_INTERVAL. Must be positive integer seconds."
  exit 1
fi

if [[ "$TIME_WAS_SET" != "true" ]]; then
  echo "Warning: --time not provided; using default ${RUNNING_TIME}s"
fi

IFS=',' read -r -a PATTERNS <<< "$PATTERN"
if [ "${#PATTERNS[@]}" -eq 0 ]; then
  echo "Invalid --pattern. Expected comma-separated voter sets."
  exit 1
fi

PATTERN_TARGETS=()
for raw_set in "${PATTERNS[@]}"; do
  trimmed="${raw_set#"${raw_set%%[![:space:]]*}"}"
  trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
  if [ -n "$trimmed" ]; then
    PATTERN_TARGETS+=("$trimmed")
  fi
done

if [ "${#PATTERN_TARGETS[@]}" -eq 0 ]; then
  echo "Invalid --pattern. No valid voter sets found."
  exit 1
fi

if [ "$RUNNING_TIME" -lt "$RECONFIG_INTERVAL" ]; then
  echo "No reconfiguration events generated. Ensure --time >= --reconfig-interval."
  exit 1
fi

RECONFIG_EVENT_COUNT=$((RUNNING_TIME / RECONFIG_INTERVAL))

echo ""
echo "Reconfiguration experiment for $RUNNING_TIME seconds with configurations:"
echo ""
echo "Consensus settings:"
echo "Protocol:          $PROTOCOL"
echo "Reconfig Mode:     $RECONFIG_MODE"
echo "Batch Size:        $BATCH_LOW-$BATCH_HIGH"
echo "Backoff Decrement: $BACKOFF_DEC"
echo ""
echo "Timeout settings:"
echo "Consensus:    $TIMEOUT_CONSENSUS ms"
echo "Election Min: $TIMEOUT_ELECTION_MIN ms"
echo "Election Max: $TIMEOUT_ELECTION_MAX ms"
echo "Batch:        $TIMEOUT_BATCH ms"
echo "Heartbeat:    $TIMEOUT_HEARTBEAT ms"
echo ""
echo "Reconfiguration settings:"
echo "Pattern:            $PATTERN"
echo "Reconfig Interval:  ${RECONFIG_INTERVAL}s"
echo "Scheduled Events:   $RECONFIG_EVENT_COUNT"
echo "Retries:            $RETRIES"
echo "Retry Delay:        ${RETRY_DELAY_MS}ms"
echo "Reconfigure Timeout:${RECONFIGURE_TIMEOUT_MS}ms"
echo "Client Grace:       ${RECONFIG_CLIENT_GRACE}s"
echo ""
echo "Press ENTER to continue..."
read

echo "Running servers..."
PUBLIC_IPS=($(jq -r '.public_ips.value[]' ./../instance_ips.json))

for i in "${!PUBLIC_IPS[@]}"; do
  id=$(printf "\\$(printf '%03o' $((65 + i)))")
  ip="${PUBLIC_IPS[$i]}"

  echo "Starting $APP_EXEC on $ip with ID $id..."
  # Ensure old session/logs do not leak into this run.
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux kill-session -t app_session >/dev/null 2>&1 || true; rm -f out.txt"
  if [ "$LOGGING" = "true" ]; then
    ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux new-session -d -s app_session './$APP_EXEC --id $id --config-file \"./nodes.json\" --protocol $PROTOCOL --reconfig-mode $RECONFIG_MODE --batch-low $BATCH_LOW --batch-high $BATCH_HIGH --backoff-decrement $BACKOFF_DEC --ct $TIMEOUT_CONSENSUS --et-min $TIMEOUT_ELECTION_MIN --et-max $TIMEOUT_ELECTION_MAX --bt $TIMEOUT_BATCH --ht $TIMEOUT_HEARTBEAT --logs > out.txt 2>&1'"
  else
    ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux new-session -d -s app_session './$APP_EXEC --id $id --config-file \"./nodes.json\" --protocol $PROTOCOL --reconfig-mode $RECONFIG_MODE --batch-low $BATCH_LOW --batch-high $BATCH_HIGH --backoff-decrement $BACKOFF_DEC --ct $TIMEOUT_CONSENSUS --et-min $TIMEOUT_ELECTION_MIN --et-max $TIMEOUT_ELECTION_MAX --bt $TIMEOUT_BATCH --ht $TIMEOUT_HEARTBEAT'"
  fi
done

echo "Done running servers. Waiting 10 seconds for connectivity setup."
sleep 10
echo "Done waiting."

CLIENT_IP=$(jq -r '.client_ip.value' ./../instance_ips.json)
echo "Running reconfiguration client on $CLIENT_IP"

# Ensure old session/log does not leak into this run.
ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux kill-session -t reconfig_session >/dev/null 2>&1 || true; rm -f out_reconfig.txt"
ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux new-session -d -s reconfig_session './$RECONFIG_EXEC --config-file \"./nodes.json\" --pattern \"$PATTERN\" --reconfig-interval $RECONFIG_INTERVAL --time $RUNNING_TIME --retries $RETRIES --retry-delay-ms $RETRY_DELAY_MS --reconfigure-timeout-ms $RECONFIGURE_TIMEOUT_MS > out_reconfig.txt 2>&1'"
if ! ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux has-session -t reconfig_session >/dev/null 2>&1"; then
  echo "Failed to start reconfiguration client session on $CLIENT_IP"
  exit 1
fi

echo "Reconfiguration client started. Waiting $RUNNING_TIME seconds."
sleep "$RUNNING_TIME"

echo "Experiment reached target time. Waiting up to ${RECONFIG_CLIENT_GRACE}s for reconfig client to finish..."
for ((i=0; i<RECONFIG_CLIENT_GRACE; i++)); do
  if ! ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux has-session -t reconfig_session >/dev/null 2>&1"; then
    break
  fi
  sleep 1
done

if ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux has-session -t reconfig_session >/dev/null 2>&1"; then
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux send-keys -t reconfig_session C-c"
fi

echo "Stopping servers."

for ip in "${PUBLIC_IPS[@]}"; do
  echo "Stopping app on $ip..."
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux send-keys -t app_session C-c"
done

N=${#PUBLIC_IPS[@]}
output_file="./../output/reconfig-protocol=$PROTOCOL-mode=$RECONFIG_MODE-N=$N-interval=$RECONFIG_INTERVAL-time=$RUNNING_TIME.txt"

echo "Downloading reconfiguration client output..."
scp -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP:~/out_reconfig.txt" "$output_file"
append_script_latency_summary "$output_file"
echo "Latency summary:"
tail -n 8 "$output_file"

if [ "$LOGGING" = "true" ]; then
  echo "Downloading logs from servers..."
  for i in "${!PUBLIC_IPS[@]}"; do
    id=$(printf "\\$(printf '%03o' $((65 + i)))")
    ip="${PUBLIC_IPS[$i]}"
    scp -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip:~/out.txt" "./../output/reconfig-logs_$id.txt"
  done
fi

echo "Done."
