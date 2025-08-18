#!/bin/bash

USER="ec2-user"
KEY="key"
APP_EXEC_NAME="app"
FAILURE_CLIENT_EXEC_NAME="failure-client"
YCSB_EXEC_NAME="go-ycsb"

PORT=8080
HTTP_PORT=7080

USE_PRIVATE_IP=$1

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <use_private_ip: true|false>"
  exit 1
fi

# Normalize input (e.g., TRUE → true)
USE_PRIVATE_IP=$(echo "$USE_PRIVATE_IP" | tr '[:upper:]' '[:lower:]')

# Validate input
if [[ "$USE_PRIVATE_IP" != "true" && "$USE_PRIVATE_IP" != "false" ]]; then
  echo "Error: <use_private_ip> must be 'true' or 'false'"
  exit 1
fi

# Load config.env
echo "Generating config files..."

# Read IPs from JSON
PUBLIC_IPS=($(jq -r '.public_ips.value[]' instance_ips.json))
PRIVATE_IPS=($(jq -r '.private_ips.value[]' instance_ips.json))

# Build nodes array
nodes=()
if [[ "$USE_PRIVATE_IP" == "true" ]]; then
  echo "Using private IPs..."
  for i in "${!PRIVATE_IPS[@]}"; do
    id="$((i + 1))"
    ip="${PRIVATE_IPS[$i]}"
    nodes+=("{\"id\":\"$id\",\"port\":\"$PORT\",\"http_port\":\"$HTTP_PORT\",\"ip\":\"$ip\"}")
  done
else
  echo "Using public IPs..."
  for i in "${!PUBLIC_IPS[@]}"; do
    id="$((i + 1))"
    ip="${PUBLIC_IPS[$i]}"
    nodes+=("{\"id\":\"$id\",\"port\":\"$PORT\",\"http_port\":\"$HTTP_PORT\",\"ip\":\"$ip\"}")
  done
fi

# Join nodes with commas
nodes_str=$(IFS=,; echo "${nodes[*]}")

# Write rcp_config
echo "rcp.config={\"nodes\":[${nodes_str}]}" > ./run/rcp_config

# Write nodes.json
cat > ./run/nodes.json <<EOF
{
  "nodes": [
    $nodes_str
  ]
}
EOF

echo "Finished generating config files."

for ip in "${PUBLIC_IPS[@]}"; do
  echo "Uploading server executable to $ip..."
  scp -i "$KEY" -o StrictHostKeyChecking=no "$APP_EXEC_NAME" "$USER@$ip:~/"

  echo "Uploading nodes.json to $ip..."
  scp -i "$KEY" -o StrictHostKeyChecking=no ./run/nodes.json "$USER@$ip:~/"

  echo "Ensuring tmux is installed on $ip..."
  ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$ip" "command -v tmux >/dev/null 2>&1 || sudo yum install -y tmux"
done

CLIENT_IP=($(jq -r '.client_ip.value' instance_ips.json))

echo "Uploading client executables to $CLIENT_IP..."
scp -i "$KEY" -o StrictHostKeyChecking=no "$FAILURE_CLIENT_EXEC_NAME" "$USER@$CLIENT_IP:~/"
scp -i "$KEY" -o StrictHostKeyChecking=no "$YCSB_EXEC_NAME" "$USER@$CLIENT_IP:~/"

echo "Uploading nodes.json, rcp_config, and workload to client..."
scp -i "$KEY" -o StrictHostKeyChecking=no ./run/nodes.json "$USER@$CLIENT_IP:~/"
scp -i "$KEY" -o StrictHostKeyChecking=no ./run/rcp_config "$USER@$CLIENT_IP:~/"
scp -i "$KEY" -o StrictHostKeyChecking=no ./run/workload "$USER@$CLIENT_IP:~/"

echo "Ensuring tmux is installed on $CLIENT_IP..."
ssh -i "$KEY" -o StrictHostKeyChecking=no "$USER@$CLIENT_IP" "command -v tmux >/dev/null 2>&1 || sudo yum install -y tmux"

echo "Done."
