import json
import subprocess
import os
import sys
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--protocol', type=str, default="rcp", help="Protocol. can be rcp/raft/fraft")
parser.add_argument('--persist', action='store_true', help="Persistent or in-memory")

args = parser.parse_args()

protocol = args.protocol
if protocol not in ["rcp", "raft", "fraft"]:
    print(f"Invalid protocol: {protocol}")
    exit(0)
persist = ""
if args.persist:
    persist = "--persist"
# Read JSON file
json_file = "nodes.json"

with open(json_file, "r") as file:
    servers = json.load(file)["nodes"]

# Function to open a new terminal and run the Go server
def run_server(server):
    # Get the current working directory
    cwd = os.getcwd()
    try:
        os.remove(f"./dbs/{server['id']}.db")
    except Exception as e:
        pass
        # print(f"Failed to clear db: {e}")
    # Command to open a new macOS Terminal window and execute the Go run command
    cmd = f'osascript -e \'tell application "Terminal" to do script "cd {cwd} && go run main.go --id {server["id"]} --logs --protocol {protocol} {persist}"\''
    subprocess.run(cmd, shell=True)

# Start each server in a new macOS terminal window
for server in servers:
    os.makedirs("./dbs", exist_ok=True)
    run_server(server)
    # time.sleep(1)  # Small delay to prevent overwhelming the system
