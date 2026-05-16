# Reconfigurable Consensus Protocol

## Requirements

- Go 1.23.x (1.23.6 recommended)

### Command Line Arguments

| Flag                  | Type   | Default          | Description                                                    |
| --------------------- | ------ | ---------------- | -------------------------------------------------------------- |
| `--id`                | string | `""`             | Node ID (must match an entry in `nodes.json`)                  |
| `--logs`              | bool   | `false`          | Enable logging                                                 |
| `--protocol`          | string | `"rcp"`          | Consensus protocol (`raft/fraft/rcp`)                          |
| `--reconfig-mode`     | string | `"none"`         | Reconfiguration mode (`none/joint/recraft/orca`)               |
| `--persist`           | bool   | `false`          | Persistent or in-memory storage                                |
| `--config`            | string | `""`             | Inline JSON string for node config (overrides `--config-file`) |
| `--config-file`       | string | `"./nodes.json"` | Path to JSON config file                                       |
| `--K`                 | int    | `2`              | Value of K                                                     |
| `--batch-low`         | int    | `100`            | Batch size to trigger AppendEntries                            |
| `--batch-high`        | int    | `200`            | Maximum batch size per AppendEntries                           |
| `--backoff-decrement` | int    | `200`            | Backoff decrement when new leader arises                       |
| `--ct`                | int    | `1000`           | Consensus timeout in milliseconds                              |
| `--et-min`            | int    | `500`            | Minimum election timeout in milliseconds                       |
| `--et-max`            | int    | `1000`           | Maximum election timeout in milliseconds                       |
| `--bt`                | int    | `2`              | Batch timeout in milliseconds                                  |
| `--ht`                | int    | `50`             | Heartbeat timeout in milliseconds                              |

## Setting up servers

### Docker

- Build docker image `docker build -t rcp .`
- Run docker container `docker run -p {host_port}:{container_http_server_port} -p {host_port:container_grpc_server_port} rcp --id {node_id} --logs --config='{config JSON here}'`
  - Example `docker run -p 8080:8080 -p 7080:7080 rcp --id S1 --logs --config='{"K":2,"batch_size":100,"nodes":[{"id":"S1","port":":8080","http_port":":7080","ip":"127.0.0.1"},{"id":"S2","port":":8081","http_port":":7081","ip":"127.0.0.1"}]}'`

### MacOS

- Run `go mod tidy` to install the required dependencies
- Modify `nodes.json` to set nodes (IDs, ports, IPs)
- Run `python3 init.py --protocol rcp` which will open up a terminal window for each node

### Manual (no Docker, no Python)

- Run `go mod tidy` to install the required dependencies
- Modify `nodes.json` to set your nodes (IDs, ports, IPs)
- Start each node in its own terminal window
  - Example (3 nodes):
    - `go run main.go --id A --logs --protocol rcp --reconfig-mode none --config-file ./nodes.json`
    - `go run main.go --id B --logs --protocol rcp --reconfig-mode none --config-file ./nodes.json`
    - `go run main.go --id C --logs --protocol rcp --reconfig-mode none --config-file ./nodes.json`
- You can optionally override config per-node with `--config '{...}'` or adjust timeouts/batching with the flags above

## Reconfiguration

- Reconfiguration APIs are gated behind `--protocol=raft`.
- `--reconfig-mode` must be one of `joint`, `recraft`, or `orca` to enable reconfiguration.
- Using `--reconfig-mode` with `--protocol=fraft` or `--protocol=rcp` is rejected at startup.
- A `Reconfigure` request appends two `RECONFIG` log entries (`transition` and `finalize`) and waits for commit.
- JOINT transition quorum behavior is applied to election and commit logic.
- RECRAFT transition quorum behavior is applied to election and commit logic for pure add and pure remove changes.
- RECRAFT mixed add+remove and ORCA transition quorum behavior are not yet applied.

## YCSB (go-ycsb-rcp)

- From the `go-ycsb-rcp` folder, build the binary:
  - `make`
- Run the workload (example):
  - `./bin/go-ycsb load orca -P workloads/workloada -P config/orca_config -p "threadcount=64" --interval 1`

## Failure Client

- Failure client is used to cause failures at specified times
- In the `failure_client` directory
- Failure client is used to cause failures at specified times
- Use the `failure_config.json` to configure the nodes and the failures
- `cd failure_client` and `go run . --config-file "./../nodes.json" --failures "5:leader,10:non-leader,15:random,20:revive"` to run the failure client
- Failures can be of `leader` or `non-leader` or `random`. The `time` is the time in seconds to wait from when the client started (with `go run .`) to cause the failure

## Reconfiguration Client

- Reconfiguration client is used to schedule reconfiguration requests at specified times.
- In the `reconfig_client` directory, run:
- `go run . --config-file "./../nodes.json" --reconfigs "5:A|B|C,15:A|C|D"`
- Format is `time:v1|v2|v3`, where `time` is seconds since client start and voters are target node IDs.
- Optional retry controls:
- `--retries` (default `100`)
- `--retry-delay-ms` (default `250`)
