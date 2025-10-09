#!/bin/bash

TARGET_DIR="terraform"

if [[ ! -d "$TARGET_DIR" ]]; then
  echo "Directory '$TARGET_DIR' does not exist."
  exit 1
fi

echo "Cleaning Terraform state and cache under ./$TARGET_DIR ..."

# Delete all .terraform directories
find "$TARGET_DIR" -type d -name ".terraform" -prune -exec rm -rf {} +

# Delete lock and state files
find "$TARGET_DIR" -type f \( \
  -name ".terraform.lock.hcl" \
  -o -name "terraform.tfstate" \
  -o -name "terraform.tfstate.backup" \
\) -exec rm -f {} +

echo "Cleaning generated file."

rm instance_ips.json

rm run/nodes.json
rm run/rcp_config

echo "Cleanup complete."
