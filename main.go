package main

import (
	"flag"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"rcp/node"
	"rcp/rcppb"
)

var (
	nodeId   = flag.String("id", "", "Node ID")
	logs     = flag.Bool("logs", false, "Logging")
	protocol = flag.String("protocol", "rcp", "raft/rcp")
)

func main() {
	flag.Parse()

	if !*logs {
		log.SetOutput(io.Discard)
	}

	if *nodeId == "" {
		log.Fatalf("Node ID is required")
	}

	if *protocol != "rcp" && *protocol != "raft" {
		log.Fatalf("protocol can either 'rcp' or 'raft'")
	}

	node, err := node.NewNode(*nodeId, *protocol)
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
