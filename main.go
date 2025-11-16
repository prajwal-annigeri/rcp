package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"rcp/grpc/orcapb"
	"rcp/node"

	"google.golang.org/grpc"
)

var (
	nodeId             = flag.String("id", "", "Node ID")
	logs               = flag.Bool("logs", false, "Logging")
	protocol           = flag.String("protocol", "rcp", "raft/fraft/rcp")
	persist            = flag.Bool("persist", false, "Persistent or in-memory")
	configFlag         = flag.String("config", "", "node config JSON")
	configFile         = flag.String("config-file", "./nodes.json", "node config JSON filename")
	K                  = flag.Int("K", 2, "Value of K")
	batchSizeLow       = flag.Int("batch-low", 100, "Batch size of new request to trigger AppendEntries")
	batchSizeHigh      = flag.Int("batch-high", 200, "Maximum batch size per AppendEntries")
	backoffDec         = flag.Int("backoff-decrement", 200, "Backoff decrement when new leader arise")
	consensusTimeout   = flag.Int("ct", 1000, "Consensus timeout in milliseconds")
	electionTimeoutMin = flag.Int("et-min", 500, "Minimum election timeout in milliseconds")
	electionTimeoutMax = flag.Int("et-max", 1000, "Maximum election timeout in milliseconds")
	batchTimeout       = flag.Int("bt", 2, "Batch timeout in milliseconds")
	heartbeatTimeout   = flag.Int("ht", 50, "Heartbeat timeout in milliseconds")
)

func main() {
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lshortfile)

	if !*logs {
		log.SetOutput(io.Discard)
	}

	nodeCfg := node.NodeConfig{
		NodeID:             *nodeId,
		Protocol:           *protocol,
		Persistent:         *persist,
		ConfigJSON:         *configFlag,
		ConfigFile:         *configFile,
		K:                  *K,
		BatchSizeLow:       *batchSizeLow,
		BatchSizeHigh:      *batchSizeHigh,
		BackoffDec:         *backoffDec,
		ConsensusTimeout:   *consensusTimeout,
		ElectionTimeoutMin: *electionTimeoutMin,
		ElectionTimeoutMax: *electionTimeoutMax,
		BatchTimeout:       *batchTimeout,
		HeartbeatTimeout:   *heartbeatTimeout,
	}

	if err := nodeCfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	nodeInstance, err := node.NewNode(nodeCfg)
	if err != nil {
		log.Fatalf("Error creating node: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", nodeInstance.Port))
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", nodeInstance.Port, err)
	}
	log.Printf("Listening on port: %v\n", nodeInstance.Port)

	grpcSrv := grpc.NewServer()

	orcapb.RegisterOrcaServer(grpcSrv, nodeInstance)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	if err := nodeInstance.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	nodeInstance.RunInteractiveMenu()
}
