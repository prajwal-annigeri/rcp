import json
import subprocess
import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--runtime-config', type=str, default="runtime.conf", help="Path to runtime.conf")
args = parser.parse_args()

runtime_config = args.runtime_config
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
    cmd = f'osascript -e \'tell application "Terminal" to do script "cd {cwd} && go run main.go --id {server["id"]} --logs --runtime-config {runtime_config}"\''
    subprocess.run(cmd, shell=True)

# Start each server in a new macOS terminal window
for server in servers:
    os.makedirs("./dbs", exist_ok=True)
    run_server(server)
    # time.sleep(1)  # Small delay to prevent overwhelming the system
