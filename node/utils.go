package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/rcppb"
	"strings"
	// "sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (node *Node) establishConns() error {
	// Iterate through every node in nodeMap and create gRPC clients for every other node
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

// utility function to print state of node
func (node *Node) printState() {
	log.Printf("Term: %d\nPrev Term: %d, Prev Log Index: %d isLeader: %t\n", node.currentTerm, node.lastTerm, node.lastIndex, node.isLeader)
	log.Printf("Current alive: %d", node.currAlive)
	log.Printf("Reachable nodes: %v", node.reachableNodes)
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

func (node *Node) forwardToLeader(KV *rcppb.KV) {

	// Construct the HTTP request
	leader, ok := node.votedFor.Load(node.currentTerm)
	if !ok {
		log.Println("BUG")
		return
	}
	client, ok := node.ClientMap[leader.(string)]
	if ok {
		client.Store(context.Background(), &rcppb.KV{Key: KV.Key, Value: KV.Value})
		log.Printf("Forwarded req with key: %s, value: %s to leader: %s\n", KV.Key, KV.Value, leader.(string))
	}
}

func (node *Node) checkInsertRecoveryLog(nodeId string) {
	node.recoverySetLock.Lock()
	defer node.recoverySetLock.Unlock()
	status, ok := node.serverStatusMap.Load(nodeId)
	
	if ok && !status.(bool) {
		_, exists := node.recoveryLogWaitingSet[nodeId]
		if !exists {
			node.recoveryLogWaitingSet[nodeId] = struct{}{}
			log.Printf("Inserting %s recovery log", nodeId)
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "recovery",
				NodeId:  nodeId,
				Term:    node.currentTerm,
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
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "failure",
				NodeId:  nodeId,
				Term:    node.currentTerm,
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


// var incMutex = sync.Mutex{}
// func (node *Node) increaseReplicationCount(index int64) {
	
// 	incMutex.Lock()
// 	defer incMutex.Unlock()
// 	cnt, ok := node.replicatedCount.Load(index)
// 	if !ok {
// 		log.Println("BUG increaseReplicationCount")
// 		return
// 	}
// 	node.replicatedCount.Store(index, cnt.(int) + 1)
// 	log.Printf("Increased replication count of index %d to %d", index, cnt.(int) + 1)
// 	if cnt.(int) + 1 == node.K + 1 {
// 		node.commitIndex = max(node.commitIndex, index)
// 	}
// }
