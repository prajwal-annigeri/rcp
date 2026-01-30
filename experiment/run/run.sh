#!/bin/bash

KEY="./../key"
APP_EXEC="app"
FAILURE_EXEC="failure-client"
YCSB_EXEC="go-ycsb"
USER="ec2-user"

# Default values
PROTOCOL="rcp"
K=2
BATCH_LOW=100
BATCH_HIGH=200
BACKOFF_DEC=100
TIMEOUT_CONSENSUS=1000
TIMEOUT_ELECTION_MIN=500
TIMEOUT_ELECTION_MAX=1000
TIMEOUT_BATCH=2
TIMEOUT_HEARTBEAT=50
LOGGING="false"
PERSISTENT="false"

CONCURRENT_CLIENT=8
FAILURE_TYPE="None"
FAILURE_TIME=5
VERBOSE="false"

RUNNING_TIME=30

while [[ $# -gt 0 ]]; do
  case "$1" in
    --protocol)
      PROTOCOL="$2"
      shift 2
      ;;
    --K)
      K="$2"
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
    --client)
      CONCURRENT_CLIENT="$2"
      shift 2
      ;;
    --failure)
      FAILURE_TYPE="$2"
      shift 2
      ;;
    --failure-time)
      FAILURE_TIME="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE="$2"
      shift 2
      ;;
    --time)
      RUNNING_TIME="$2"
      shift 2
      ;;
    *)
      echo "Unknown or malformed option: $1"
      exit 1
      ;;
  esac
done

echo ""

echo "Experiment to run for $RUNNING_TIME seconds with configurations:"

echo ""

echo "Consensus settings:"
echo "Protocol:          $PROTOCOL"
echo "K:                 $K"
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

echo "Client settings:"
echo "Concurrent Client:    $CONCURRENT_CLIENT"
echo "Failure Type:         $FAILURE_TYPE"
echo "Time Between Failure: $FAILURE_TIME"
echo "Verbose:              $VERBOSE"

echo ""

echo "Press ENTER to continue..."
read

RUNTIME_CONF="./runtime.conf"

cat > "$RUNTIME_CONF" <<EOF
protocol=$PROTOCOL
persistent=$PERSISTENT
k=$K
batch_size_low=$BATCH_LOW
batch_size_high=$BATCH_HIGH
backoff_decrement=$BACKOFF_DEC
consensus_timeout_ms=$TIMEOUT_CONSENSUS
election_timeout_min_ms=$TIMEOUT_ELECTION_MIN
election_timeout_max_ms=$TIMEOUT_ELECTION_MAX
batch_timeout_ms=$TIMEOUT_BATCH
heartbeat_timeout_ms=$TIMEOUT_HEARTBEAT
EOF

# Run servers
echo "Running servers..."

PUBLIC_IPS=($(jq -r '.public_ips.value[]' ./../instance_ips.json))

for i in "${!PUBLIC_IPS[@]}"; do
  id=$(printf "\\$(printf '%03o' $((65 + i)))")
  ip="${PUBLIC_IPS[$i]}"

  echo "Uploading runtime.conf to $ip..."
  scp -i "$KEY" -o StrictHostKeyChecking=no "$RUNTIME_CONF" "$USER@$ip:~/runtime.conf"

  echo "Starting $APP_EXEC on $ip with ID $id..."
  if [ "$LOGGING" = "true" ]; then
    ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux new-session -d -s app_session './$APP_EXEC --id $id --config-file \"./nodes.json\" --logs > out.txt 2>&1'"
  else
    ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux new-session -d -s app_session './$APP_EXEC --id $id --config-file \"./nodes.json\"'"
  fi
done

echo "Done running servers. Waiting 10 seconds to make sure servers have established connections before running client."
sleep 10
echo "Done waiting."

# Run client
CLIENT_IP=($(jq -r '.client_ip.value' ./../instance_ips.json))

echo "Running client on $CLIENT_IP"

echo "Starting $YCSB_EXEC"
ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux new-session -d -s ycsb_session './$YCSB_EXEC load rcp -P workload -P rcp_config -p \"threadcount=$CONCURRENT_CLIENT\" -p \"verbose=$VERBOSE\" --interval 1 > out.txt'"

N=${#PUBLIC_IPS[@]}
failures=""

if [[ "$FAILURE_TYPE" == "LF" ]]; then
  # Leader failure
  for (( i=FAILURE_TIME; i<=(N-K)*FAILURE_TIME; i+=FAILURE_TIME )); do
    failures+="$i:leader,"
  done
  failures=${failures%,}

elif [[ "$FAILURE_TYPE" == "RF" ]]; then
  # Replica failure
  for (( i=FAILURE_TIME; i<=(N-K)*FAILURE_TIME; i+=FAILURE_TIME )); do
    failures+="$i:non-leader,"
  done
  failures=${failures%,}

elif [[ "$FAILURE_TYPE" == "LOF" ]]; then
  # Oscillating failure
  for (( i=FAILURE_TIME; i<RUNNING_TIME; i+=FAILURE_TIME*2 )); do
    failures+="$i:leader,$((i+FAILURE_TIME)):revive,"
  done
  failures=${failures%,}

elif [[ "$FAILURE_TYPE" == "ROF" ]]; then
  # Oscillating failure
  for (( i=FAILURE_TIME; i<RUNNING_TIME; i+=FAILURE_TIME*2 )); do
    failures+="$i:non-leader,$((i+FAILURE_TIME)):revive,"
  done
  failures=${failures%,}

fi

echo "Warming up for 30 seconds"
sleep 30

if [[ ${#failures} -ne 0 ]]; then
  echo "Done warming up, starting failure client..."
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux new-session -d -s failure_session './$FAILURE_EXEC --config-file \"./nodes.json\" --failures \"$failures\"'"
  echo "Failure client started, running for $RUNNING_TIME seconds"
  sleep $RUNNING_TIME
else
  echo "No failure, running for $RUNNING_TIME seconds"
  sleep $RUNNING_TIME
fi

# Stopping client and servers
echo "Experiment is done, stopping client and servers."

echo "Stopping client on $CLIENT_IP..."
ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux send-keys -t ycsb_session C-c"

if [[ ${#failures} -ne 0 ]]; then
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "tmux send-keys -t failure_session C-c"
fi

for ip in "${PUBLIC_IPS[@]}"; do
  echo "Stopping app on $ip..."
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "tmux send-keys -t app_session C-c"
done

echo "Downloading output from client..."
scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$CLIENT_IP:~/out.txt "./../output/protocol=$PROTOCOL-N=$N-K=$K-client=$CONCURRENT_CLIENT-batch=$BATCH_LOW,$BATCH_HIGH-fail=$FAILURE_TYPE-bt=$TIMEOUT_BATCH.txt"


if [ "$LOGGING" = "true" ]; then
  echo "Downloading logs from servers..."

  for i in "${!PUBLIC_IPS[@]}"; do
    id=$(printf "\\$(printf '%03o' $((65 + i)))")
    ip="${PUBLIC_IPS[$i]}"

    scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$ip:~/out.txt "./../output/logs_$id.txt"
  done
fi

# Done
echo "Done."
