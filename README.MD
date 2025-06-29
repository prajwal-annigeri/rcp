# Reconfigurable Consensus Protocol

## Requirements
- Go 1.23.x

### Command Line Arguments
| Flag          | Type   | Default         | Description                          |
|---------------|--------|-----------------|--------------------------------------|
| `--id`        | string | `""`            | Node ID                              |
| `--logs`      | bool   | `false`         | Enable logging                       |
| `--protocol`  | string | `"rcp"`         | Consensus protocol (`raft/fraft/rcp`) |
| `--persist`   | bool   | `false`         | Use persistent storage or in-memory  |
| `--config`    | string | `""`            | Inline JSON string for node config   |
| `--config-file` | string | `"./nodes.json"` | Path to JSON config file           |


## Setting up servers

### Docker
- Build docker image `docker build -t rcp .`
- Run docker container `docker run -p {host_port}:{container_http_server_port} -p {host_port:container_grpc_server_port} rcp --id {node_id} --logs --config='{config JSON here}'`
    - Example `docker run -p 8080:8080 -p 7080:7080 rcp --id S1 --logs --config='{"K":2,"nodes":[{"id":"S1","port":":8080","http_port":":7080","ip":"127.0.0.1"},{"id":"S2","port":":8081","http_port":":7081","ip":"127.0.0.1"}]}'`

### MacOS
- Run `go mod tidy` to install the required dependencies
- Modify `nodes.json` to set number of nodes and value of 'K'
- Run `python3 init.py` which will open up a terminal window for each node

## Failure Client
- Failure client is used to cause failures at specified times
- In the `failure_client` directory
- Failure client is used to cause failures at specified times
- Use the `failure_config.json` to configure the nodes and the failures
- `cd failure_client` and `go run .` to run the failure client
- Failures can be of `leader` or `non-leader` or `random`. The `time` is the time in seconds to wait from when the client started (with `go run .`)  to cause the failure
