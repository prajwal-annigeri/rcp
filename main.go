package main

import (
	"flag"
	"io"
	"log"
	"net"
	"rcp/rcppb"
	"rcp/node"
	"google.golang.org/grpc"
)

var (
	nodeId = flag.String("id", "", "Node ID")
	logs   = flag.Bool("logs", false, "Logging")
)

func main() {
	flag.Parse()

	if !*logs {
		log.SetOutput(io.Discard)
	}

	if *nodeId == "" {
		log.Fatalf("Node ID is required")
	}

	node, err := node.NewNode(*nodeId)
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
