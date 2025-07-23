package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/rcppb"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (node *Node) establishConns() error {
	// Iterate through every node in nodeMap and create gRPC clients for every other node
	for id, address := range node.NodeAddressMap {
		if node.Id != id && node.ConnMap[id] == nil {
			if node.NodeAddressMap[id] == "" {
				return errors.New("no node:address mapping")
			}
			log.Printf("Establishing connection from %s to %s (%s)\n", node.Id, id, address)
			var conn *grpc.ClientConn
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			client := rcppb.NewRCPClient(conn)
			node.ClientMap[id] = client
			node.ConnMap[id] = conn
			go node.checkHealth(id)
		}
	}
	return nil
}

func (node *Node) checkHealth(nodeID string) {
	grpcClient, ok := node.ClientMap[nodeID]
	if !ok {
		log.Printf("BUG() checkHealth() gRPCClient should have been in map")
		return
	}

	for {
		_, err := grpcClient.Healthz(context.Background(), &rcppb.HealthzRequest{})
		if err == nil {
			connectedNodes := node.initialConnectionEstablished.Add(1)
			log.Printf("Connected to %s!", nodeID)
			// If connections with all other nodes is established, go ready
			if connectedNodes+1 == int64(len(node.NodeAddressMap)) {
				node.isReady = true
			}
			return
		}

		time.Sleep(2 * time.Millisecond)
	}
}

func printMenu() {
	fmt.Println("\nMenu:")
	fmt.Println("2. Print Log")
	fmt.Println("3. Print All Logs Unordered")
	fmt.Println("4. Print State")
	fmt.Println("0. Exit")
	fmt.Print("Choose an option: ")
}

// utility function to print state of node
func (node *Node) printState() {
	log.Printf("Term: %d\nPrev Term: %d, Prev Log Index: %d isLeader: %t\n", node.currentTerm, node.lastTerm, node.lastIndex, node.isLeader)
	log.Printf("Current alive: %d", node.currAlive)
	log.Printf("Reachable nodes: %v", node.reachableNodes)
	log.Printf("Persistent: %t", node.isPersistent)
	log.Println("Next Index: ")
	var nextIndexString strings.Builder
	node.nextIndex.Range(func(key, value any) bool {
		nextIndexString.WriteString(fmt.Sprintf("%s: %v, ", key, value))
		return true
	})
	log.Printf("%s", nextIndexString.String())
	log.Println("Server status: ")
	var serverStatusString strings.Builder
	node.serverStatusMap.Range(func(server, status any) bool {
		serverStatusString.WriteString(fmt.Sprintf("%s: %t, ", server, status))
		return true
	})
	log.Printf("%s", serverStatusString.String())
}

// func (node *Node) forwardToLeader(storeReq *rcppb.StoreRequest) {

// 	// Construct the HTTP request
// 	leader, ok := node.votedFor.Load(node.currentTerm)
// 	if !ok {
// 		log.Println("BUG")
// 		return
// 	}
// 	client, ok := node.ClientMap[leader.(string)]
// 	if ok {
// 		client.Store(context.Background(), &rcppb.StoreRequest{Key: storeReq.Key, Value: storeReq.Value, Bucket: storeReq.Bucket})
// 		log.Printf("Forwarded req with key: %s, value: %s to leader: %s\n", storeReq.Key, storeReq.Value, leader.(string))
// 	}
// }

func (node *Node) checkInsertRecoveryLog(nodeId string) {
	node.recoverySetLock.Lock()
	defer node.recoverySetLock.Unlock()
	status, ok := node.serverStatusMap.Load(nodeId)

	if ok && !status.(bool) {
		_, exists := node.recoveryLogWaitingSet[nodeId]
		if !exists {
			node.recoveryLogWaitingSet[nodeId] = struct{}{}
			log.Printf("Inserting %s recovery log", nodeId)
			node.logBufferChan <- LogWithCallbackChannel{
				LogEntry: &rcppb.LogEntry{
					LogType: "recovery",
					NodeId:  nodeId,
					Term:    node.currentTerm,
				},
			}
		}
	}
}

func (node *Node) checkInsertFailureLog(nodeId string) {
	node.failureSetLock.Lock()
	defer node.failureSetLock.Unlock()
	status, ok := node.serverStatusMap.Load(nodeId)
	if ok && status.(bool) {

		_, exists := node.failureLogWaitingSet[nodeId]
		if !exists {
			node.failureLogWaitingSet[nodeId] = struct{}{}
			log.Printf("Inserting %s failure log", nodeId)
			node.logBufferChan <- LogWithCallbackChannel{
				LogEntry: &rcppb.LogEntry{
					LogType: "failure",
					NodeId:  nodeId,
					Term:    node.currentTerm,
				},
			}
		}
	}
}

func (node *Node) removeFromFailureSet(nodeId string) {
	node.failureSetLock.Lock()
	defer node.failureSetLock.Unlock()
	delete(node.failureLogWaitingSet, nodeId)
}

func (node *Node) removeFromRecoverySet(nodeId string) {
	node.recoverySetLock.Lock()
	defer node.recoverySetLock.Unlock()
	delete(node.recoveryLogWaitingSet, nodeId)
}

func (node *Node) makeCallbackChannel() (string, chan struct{}) {
	id := uuid.New().String()
	callbackChannel := make(chan struct{}, 10)
	// node.callbackChannelMap.Store(id, callbackChannel)
	return id, callbackChannel
}
