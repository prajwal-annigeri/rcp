package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/grpc/orcapb"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// selectKthLargest returns the k-th largest element (1-based) in vals.
// It mutates vals in-place.
func selectKthLargest(vals []int64, k int) (int64, bool) {
	if k < 1 || k > len(vals) {
		return 0, false
	}
	target := k - 1
	left, right := 0, len(vals)-1

	for left <= right {
		pivotIndex := (left + right) / 2
		pivotIndex = partitionDesc(vals, left, right, pivotIndex)
		if pivotIndex == target {
			return vals[pivotIndex], true
		}
		if pivotIndex > target {
			right = pivotIndex - 1
		} else {
			left = pivotIndex + 1
		}
	}

	return 0, false
}

func partitionDesc(vals []int64, left, right, pivotIndex int) int {
	pivotValue := vals[pivotIndex]
	vals[pivotIndex], vals[right] = vals[right], vals[pivotIndex]
	storeIndex := left
	for i := left; i < right; i++ {
		if vals[i] > pivotValue {
			vals[storeIndex], vals[i] = vals[i], vals[storeIndex]
			storeIndex++
		}
	}
	vals[right], vals[storeIndex] = vals[storeIndex], vals[right]
	return storeIndex
}

func quorumMatchIndex(matchIndex map[string]int64, failedSet, pendingRecoverySet map[string]struct{}, required int) (int64, bool) {
	if required <= 0 {
		return 0, false
	}

	values := make([]int64, 0, len(matchIndex))
	for nodeID, idx := range matchIndex {
		if _, failed := failedSet[nodeID]; failed {
			continue
		}
		if _, pendingRecovery := pendingRecoverySet[nodeID]; pendingRecovery {
			continue
		}
		values = append(values, idx)
	}

	if len(values) < required {
		return 0, false
	}

	return selectKthLargest(values, required)
}

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
			client := orcapb.NewOrcaClient(conn)

			node.mutex.Lock()
			node.ClientMap[id] = client
			node.ConnMap[id] = conn
			node.mutex.Unlock()

			go node.checkHealth(id)
		}
	}
	return nil
}

func (node *Node) checkHealth(nodeID string) {
	node.mutex.Lock()
	grpcClient, ok := node.ClientMap[nodeID]
	node.mutex.Unlock()

	if !ok {
		log.Printf("BUG() checkHealth() gRPCClient should have been in map")
		return
	}

	for {
		_, err := grpcClient.Health(context.Background(), &emptypb.Empty{})
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
	log.Printf("Term: %d\nPrev Term: %d, Prev Log Index: %d isLeader: %t\n", node.currentTerm, node.GetLastTermLocked(), node.GetLastIndexLocked(), node.isLeader)
	log.Printf("Current alive: %d", node.N-len(node.failedSet)-len(node.pendingRecoverySet))
	// log.Printf("Reachable nodes: %v", node.reachableNodes)
	log.Println("Next Index: ")
	var nextIndexString strings.Builder

	for key, value := range node.nextIndex {
		nextIndexString.WriteString(fmt.Sprintf("%s: %v, ", key, value))
	}

	log.Printf("%s", nextIndexString.String())
	log.Println("Server status: ")
	var serverStatusString strings.Builder

	for nodeId := range node.failedSet {
		serverStatusString.WriteString(fmt.Sprintf("%s: failed, ", nodeId))
	}

	for nodeId := range node.pendingFailureSet {
		serverStatusString.WriteString(fmt.Sprintf("%s: pending failure, ", nodeId))
	}

	for nodeId := range node.pendingRecoverySet {
		serverStatusString.WriteString(fmt.Sprintf("%s: pending recovery, ", nodeId))
	}

	log.Printf("%s", serverStatusString.String())
}

// RunInteractiveMenu exposes the previous CLI loop for interactive runs.
func (node *Node) RunInteractiveMenu() {
	for {
		printMenu()
		var input string
		fmt.Scan(&input)

		switch input {
		case "2":
			node.db.PrintAllLogs()
		case "4":
			node.printState()
		default:
			fmt.Println("Invalid option. Please choose again.")
		}
	}
}
