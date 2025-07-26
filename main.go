package main

import (
	"flag"
	"io"
	"log"
	"net"
	"rcp/node"
	"rcp/rcppb"

	"google.golang.org/grpc"
)

var (
	nodeId           = flag.String("id", "", "Node ID")
	logs             = flag.Bool("logs", false, "Logging")
	protocol         = flag.String("protocol", "rcp", "raft/fraft/rcp")
	persist          = flag.Bool("persist", false, "Persistent or in-memory")
	config           = flag.String("config", "", "node config JSON")
	configFile       = flag.String("config-file", "./nodes.json", "node config JSON filename")
	K                = flag.Int("K", 2, "Value of K")
	batchSize        = flag.Int("batch-size", 100, "Maximum batch size per AppendEntries")
	consensusTimeout = flag.Int("ct", 1000, "Consensus timeout in milliseconds")
	electionTimeout  = flag.Int("et", 1000, "Election timeout in milliseconds")
	batchTimeout     = flag.Int("bt", 2, "Batch timeout in milliseconds")
	heartbeatTimeout = flag.Int("ht", 50, "Heartbeat timeout in milliseconds")
)

func main() {
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if !*logs {
		log.SetOutput(io.Discard)
	}

	if *nodeId == "" {
		log.Fatalf("Node ID is required")
	}

	if *protocol != "rcp" && *protocol != "raft" && *protocol != "fraft" {
		log.Fatalf("protocol can either 'rcp' or 'fraft' or 'raft'")
	}

	node, err := node.NewNode(*nodeId, *protocol, *persist, *config, *configFile, *K, *batchSize, *consensusTimeout, *electionTimeout, *batchTimeout, *heartbeatTimeout)
	if err != nil {
		log.Fatalf("Error creating node: %v", err)
	}

	lis, err := net.Listen("tcp", node.Port)
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", node.Port, err)
	}
	log.Printf("Listening on port: %v\n", node.Port)

	grpcSrv := grpc.NewServer()

	rcppb.RegisterRCPServer(grpcSrv, node)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	node.Start()
}
