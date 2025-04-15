package main

import (
	"log"
	"rcp/rcppb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func establishConns(config *Config) (map[string]rcppb.RCPClient, error) {
	// Iterate through every node in nodeMap and create gRPC clients for every other node
	clientMap := make(map[string]rcppb.RCPClient)
	for _, node := range config.Nodes {
		id := node.ID
		log.Printf("Establishing connection from client to %s\n", id)
		var conn *grpc.ClientConn
		conn, err := grpc.NewClient(node.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		client := rcppb.NewRCPClient(conn)
		clientMap[id] = client
	}
	return clientMap, nil
}
