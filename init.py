import json
import subprocess
import time
import os
import sys

protocol = "rcp"
if len(sys.argv) >=2:
    protocol = sys.argv[1]
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
        print(f"Failed to clear db: {e}")
    # Command to open a new macOS Terminal window and execute the Go run command
    cmd = f'osascript -e \'tell application "Terminal" to do script "cd {cwd} && go run main.go --id {server["id"]} --logs --protocol {protocol}"\''
    subprocess.run(cmd, shell=True)

# Start each server in a new macOS terminal window
for server in servers:
    os.makedirs("./dbs", exist_ok=True)
    run_server(server)
    # time.sleep(1)  # Small delay to prevent overwhelming the system
