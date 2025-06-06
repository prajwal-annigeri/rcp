import json
import random
import numpy as np

# --- Configuration ---
NUM_OPERATIONS = 500
NUM_KEYS = 100000
ZIPF_ALPHA = 1.2
NODE_IDS = ["S1"]

OPERATION_TYPES = ["KV_Store"]  # Supported KV operations

MIN_VALUE = 1
MAX_VALUE = 1000

def generate_zipfian_key(num_keys, alpha):
    rank = np.random.zipf(alpha)
    key = max(1, min(rank, num_keys))
    return f"key_{key}"

# --- Generate Test Cases ---
test_cases = []

for _ in range(NUM_OPERATIONS):
    op_type = random.choice(OPERATION_TYPES)
    key = generate_zipfian_key(NUM_KEYS, ZIPF_ALPHA)
    node_id = random.choice(NODE_IDS)

    entry = {
        "operation": op_type,
        "key": key,
        "node_id": node_id
    }

    if op_type == "KV_Store":
        entry["value"] = str(random.randint(MIN_VALUE, MAX_VALUE))

    test_cases.append(entry)

# --- Write to JSON file ---
output_file = "kv_store_requests.json"
try:
    with open(output_file, "w") as f:
        json.dump(test_cases, f, indent=4)
    print(f"Successfully wrote {len(test_cases)} operations to '{output_file}'")
except Exception as e:
    print(f"Failed to write output: {e}")
