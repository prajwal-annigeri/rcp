# Reconfigurable Consensus Protocol

## Requirements
- Go 1.23.x

### Command Line Arguments
| Flag               | Type   | Default            | Description                        |
|--------------------|--------|--------------------|------------------------------------|
| `--id`             | string | `""`               | Node ID                            |
| `--logs`           | bool   | `false`            | Enable logging                     |
| `--config`         | string | `""`               | Inline JSON string for node config |
| `--config-file`    | string | `"./nodes.json"`   | Path to JSON config file           |
| `--runtime-config` | string | `"./runtime.conf"` | Path to runtime configuration file |

### Runtime Configuration File
Runtime settings (protocol, timeouts, batch sizes, etc.) are loaded from `runtime.conf`.

Example:
```
protocol=rcp
persistent=false
k=2
batch_size_low=100
batch_size_high=200
backoff_decrement=200
consensus_timeout_ms=1000
election_timeout_min_ms=500
election_timeout_max_ms=1000
batch_timeout_ms=2
heartbeat_timeout_ms=50
```


## Setting up servers

### MacOS
- Run `go mod tidy` to install the required dependencies
- Modify `nodes.json` to set number of nodes and update `runtime.conf` for runtime settings (including `k`)
- Run `python3 init.py` which will open up a terminal window for each node

## Failure Client
- Failure client is used to cause failures at specified times
- In the `failure_client` directory
- Failure client is used to cause failures at specified times
- Use the `failure_config.json` to configure the nodes and the failures
- `cd failure_client` and `go run . --config-file "./../nodes.json" --failures "5:leader,10:non-leader,15:random,20:revive"` to run the failure client
- Failures can be of `leader` or `non-leader` or `random`. The `time` is the time in seconds to wait from when the client started (with `go run .`)  to cause the failure
