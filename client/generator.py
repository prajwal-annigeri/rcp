import json
import random
import numpy as np

# --- Configuration ---
NUM_OPERATIONS = 50      # Total number of operations to generate
NUM_ACCOUNTS = 100000     # Total number of unique account IDs available (e.g., 1 to 100)
ZIPF_ALPHA = 1.2       # Zipf distribution parameter (alpha > 1). Higher values mean more skew towards lower ranks (more popular accounts).
# NODE_IDS = ["S1", "S2", "S3", "S4", "S5"] # Available node IDs
NODE_IDS = ["S1"]
OPERATION_TYPES = [
    "KV_Store"
    # "transact_savings",
    # "delay",
    # "kill",
    # "deposit_checking",
    # "send_payment",
    # "write_check",
    # "amalgamate"
]
# Optional: Define weights if some operations should be more frequent
# OPERATION_WEIGHTS = [10, 2, 1, 10, 8, 5, 3] # Example weights

MIN_AMOUNT = 1
MAX_AMOUNT = 100
MIN_DELAY = 50
MAX_DELAY = 300

# --- Helper Function for Zipfian Account ID Generation ---
def generate_zipfian_account_id(num_accounts, alpha):
    """Generates an account ID based on Zipfian distribution."""
    # np.random.zipf generates values >= 1.
    # The value represents the rank. Lower ranks (like 1) are most frequent.
    rank = np.random.zipf(alpha)
    # Clamp the rank to be within the valid range of account IDs [1, num_accounts]
    # We map the rank directly to an account ID (assuming IDs are 1-based sequential)
    account_id = max(1, min(rank, num_accounts))
    return str(account_id) # Return as string, like in the example

# --- Test Case Generation Logic ---
test_cases = []

for _ in range(NUM_OPERATIONS):
    # Choose an operation type randomly (or use weighted choice)
    operation_type = random.choice(OPERATION_TYPES)
    # operation_type = random.choices(OPERATION_TYPES, weights=OPERATION_WEIGHTS, k=1)[0] # Weighted choice

    operation_data = {"operation": operation_type}

    # --- Populate fields based on operation type ---

    if operation_type in ["transact_savings", "deposit_checking", "write_check"]:
        operation_data["account_id"] = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        operation_data["amount"] = random.randint(MIN_AMOUNT, MAX_AMOUNT)
        operation_data["node_id"] = random.choice(NODE_IDS)

    elif operation_type == "send_payment":
        # Ensure source and destination are different
        src_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        dest_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        while dest_id == src_id:
             dest_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        operation_data["src_account_id"] = src_id
        operation_data["dest_account_id"] = dest_id
        operation_data["amount"] = random.randint(MIN_AMOUNT, MAX_AMOUNT)
        operation_data["node_id"] = random.choice(NODE_IDS)

    elif operation_type == "amalgamate":
         # Ensure source and destination are different
        src_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        dest_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        while dest_id == src_id:
             dest_id = generate_zipfian_account_id(NUM_ACCOUNTS, ZIPF_ALPHA)
        operation_data["src_account_id"] = src_id
        operation_data["dest_account_id"] = dest_id
        operation_data["node_id"] = random.choice(NODE_IDS)

    elif operation_type == "delay":
        # Ensure source and destination nodes are potentially different
        src_node = random.choice(NODE_IDS)
        dest_node = random.choice(NODE_IDS)
        # Optionally force them to be different if needed:
        # while dest_node == src_node:
        #     dest_node = random.choice(NODE_IDS)
        operation_data["src_node_id"] = src_node
        operation_data["dst_node_id"] = dest_node
        operation_data["delay"] = random.randint(MIN_DELAY, MAX_DELAY)

    elif operation_type == "kill":
        operation_data["node_id"] = random.choice(NODE_IDS)

    # Add the generated operation to our list
    test_cases.append(operation_data)

try:
    with open('generated_ops.json', 'w') as f_out:
        # Use json.dump() to write the list directly to the file object
        # indent=4 makes the JSON file human-readable (pretty-printed)
        json.dump(test_cases, f_out, indent=4)
    print(f"Successfully generated {len(test_cases)} test cases and saved to 'generated_ops,json'")

except IOError as e:
    print(f"Error writing to file 'generated_ops,json': {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
