package node

import (
	"errors"
	"fmt"
	"log"
	"rcp/rcppb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


func (node *Node) establishConns() error {
	for id, port := range node.NodeMap {
		if node.Id != id && node.ConnMap[id] == nil {
			if node.NodeMap[id] == "" {
				return errors.New("no node:port mapping")
			}
			log.Printf("Establishing connection from %s to %s\n", node.Id, id)
			var conn *grpc.ClientConn
			conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			client := rcppb.NewRCPClient(conn)
			node.ClientMap[id] = client
			node.ConnMap[id] = conn
		}
	}
	return nil
}

func printMenu() {
	fmt.Println("\nMenu:")
	fmt.Println("2. Print Log")
	fmt.Println("3. Print All Logs Unordered")
	fmt.Println("4. Print State")
	fmt.Println("0. Exit")
	fmt.Print("Choose an option: ")
}

func (node *Node) printState() {
	log.Printf("Term: %d\nPrev Term: %d, Prev Log Index: %d isLeader: %t\n", node.currentTerm, node.lastTerm, node.lastIndex, node.isLeader)
	log.Printf("Current alive: %d", node.currAlive)
	log.Println("Next Index: ")
	node.nextIndex.Range(func(key, value any) bool {
		log.Printf("%s: %v, ", key, value)
		return true
	})
	log.Println()
}
