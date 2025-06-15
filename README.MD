# Reconfigurable Consensus Protocol

## Setup locally

### Requirements
- Go 1.23.x

### Setting up servers
#### MacOS
- Run `go mod tidy` to install the required dependencies
- Modify `nodes.json` to set number of nodes and value of 'K'
- Run `python3 init.py` which will open up a terminal window for each node

### Running client
- `cd ./client`
- `go run .`
