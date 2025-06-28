package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"rcp/rcppb"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Node represents a server with its ID, ports
type Node struct {
	ID       string `json:"id"`
	Port     string `json:"port"`
	HTTPPort string `json:"http_port"`
}

// Config represents the JSON structure
type Config struct {
	K     int    `json:"K"`
	Nodes []Node `json:"nodes"`
}

// LoadConfig reads and parses the JSON file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// displayMenu shows the user menu
func displayMenu() {
	fmt.Println("\nMenu:")
	fmt.Println("0. Do operations from file")
	fmt.Println("1. Send Store Request")
	fmt.Println("2. Get Value")
	fmt.Println("3. Kill server")
	fmt.Println("4. Revive server")
	fmt.Println("5. Partition")
	fmt.Println("6. Set Delay")
	fmt.Println("7. Delete Key")
	// fmt.Println("7. Get Balance")
	// fmt.Println("8. Deposit Checking")
	// fmt.Println("9. Write Check")
	fmt.Println("10. Performance")
	fmt.Println("-1. Exit")
}

// MapServerIDToHTTPPort creates a mapping of server IDs to HTTP ports
func MapServerIDToHTTPPort(config *Config) map[string]string {
	serverMap := make(map[string]string)
	for _, node := range config.Nodes {
		serverMap[node.ID] = node.HTTPPort
	}
	return serverMap
}

// sendStoreRequest prompts for server ID, key, and value, then makes an HTTP request
func sendStoreRequest(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Node ID (e.g., S1, S2, S3): ")
	nodeID, _ := reader.ReadString('\n')
	nodeID = strings.TrimSpace(nodeID)

	// Get HTTP port for the server
	// httpPort, exists := serverMap[serverID]
	// if !exists {
	// 	fmt.Println("Invalid Server ID!")
	// 	return
	// }

	grpcClient, ok := grpcClientMap[nodeID]
	if !ok {
		log.Printf("No gRPC client for node %s", nodeID)
		return
	}

	// Ask for Key and Value
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Value: ")
	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	begin := time.Now()

	resp, err := grpcClient.Store(context.Background(), &rcppb.StoreRequest{Key: key, Value: value, Bucket: bucket})
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	log.Printf("TIME: %v", time.Since(begin))

	fmt.Println("Store request sent! Result:", resp.Value)
}

func sendDeleteRequest(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Node ID (e.g., S1, S2, S3): ")
	nodeID, _ := reader.ReadString('\n')
	nodeID = strings.TrimSpace(nodeID)

	// Get HTTP port for the server
	// httpPort, exists := serverMap[serverID]
	// if !exists {
	// 	fmt.Println("Invalid Server ID!")
	// 	return
	// }

	grpcClient, ok := grpcClientMap[nodeID]
	if !ok {
		log.Printf("No gRPC client for node %s", nodeID)
		return
	}

	// Ask for Key and Value
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	begin := time.Now()

	resp, err := grpcClient.Delete(context.Background(), &rcppb.DeleteReq{Key: key, Bucket: bucket})
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	log.Printf("TIME: %v", time.Since(begin))

	fmt.Println("Delete request sent! Result:", resp.Value)
}

func doPartition(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Enter partitions. Example: [[\"S1\", \"S2\"], [\"S3\", \"S4\"]]")
	
	input, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("Error reading input: %v", err)
        return
    }
	input = strings.TrimSpace(input)
    input = strings.ReplaceAll(input, ", ", ",")
	var partitions [][]string
	err = json.Unmarshal([]byte(input), &partitions)
	if err != nil {
		log.Printf("Error parsing partitions: %v", err)
		return
	}

	for _, partitionList := range partitions {
		for _, nodeId := range partitionList {
			grpcClient, ok := grpcClientMap[nodeId]
			if !ok {
				log.Printf("Node %s does not exist", nodeId)
				continue
			}
			_, err := grpcClient.Partition(context.Background(), &rcppb.PartitionReq{ReachableNodes: partitionList})
			if err != nil {
				log.Printf("Error setting reachable nodes on %s: %v", nodeId, err)
			}
		}
		
	}
}

type GetValueResponse struct {
	Value string `json:"value"`
}

// getValue makes Get request to query KV store
func getValue(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3) or 'all' to query all servers: ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Ask for Key
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	// Ask for Bucket
	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	if serverID == "all" {
		GetValuesFromAll(grpcClientMap, key, bucket)
	} else {
		grpcClient, ok := grpcClientMap[serverID]
		if !ok {
			fmt.Printf("Node %s does not exist", serverID)
			return
		}
		
		value, err := GetValueFrom(grpcClient, key, bucket)
		if err != nil {
			fmt.Printf("Error getting value from server %s: %v\n", serverID, err)
			return
		}
		fmt.Printf("Value on %s: %s\n", serverID, value)
	}

}

func GetValueFrom(grpcClient rcppb.RCPClient, key string, bucket string) (string, error) {
	val, err := grpcClient.Get(context.Background(), &rcppb.GetValueReq{Key: key, Bucket: bucket})
	if err != nil {
		return "", err
	}
	
	return val.Value, nil
}

// GetValuesFromAll queries all servers for a key
func GetValuesFromAll(grpcClientMap map[string]rcppb.RCPClient, key string, bucket string) {
	for serverID, grpcClient := range grpcClientMap {
		value, err := GetValueFrom(grpcClient, key, bucket)
		if err != nil {
			fmt.Printf("Error querying server %s: %v\n", serverID, err)
		} else {
			fmt.Printf("%s: %s\n", serverID, value)
		}
	}
}

func killServer(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server IDs separated by spaces (e.g., S1 S2 S3): ")
	serverIDs, _ := reader.ReadString('\n')
	serverIDs = strings.TrimSpace(serverIDs)
	serverIdSlice := strings.Fields(serverIDs)
	for _, serverID := range serverIdSlice {
		grpcClient, exists := grpcClientMap[serverID]
		if !exists {
			fmt.Printf("Invalid Server ID: %s!", serverID)
			continue
		}

		_, err := grpcClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
		if err != nil {
			log.Printf("Error killing server %s: %v", serverID, err)
		}
	}
}

func reviveServer(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server IDs separated by spaces (e.g., S1 S2 S3): ")
	serverIDs, _ := reader.ReadString('\n')
	serverIDs = strings.TrimSpace(serverIDs)
	serverIdSlice := strings.Fields(serverIDs)
	for _, serverID := range serverIdSlice {
		grpcClient, exists := grpcClientMap[serverID]
		if !exists {
			fmt.Printf("Invalid Server ID: %s!", serverID)
			continue
		}

		_, err := grpcClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
		if err != nil {
			log.Printf("Error killing server %s: %v", serverID, err)
		}
	}
}

func sendDelayRequest(grpcClientMap map[string]rcppb.RCPClient) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for From, To and Delay
	fmt.Print("Enter From: ")
	from, _ := reader.ReadString('\n')
	from = strings.TrimSpace(from)

	grpcClient, exists := grpcClientMap[from]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	fmt.Print("Enter To: ")
	to, _ := reader.ReadString('\n')
	to = strings.TrimSpace(to)

	fmt.Print("Enter Delay (in milliseconds): ")
	delayStr, _ := reader.ReadString('\n')
	delayStr = strings.TrimSpace(delayStr)

	delay, err := strconv.ParseInt(delayStr, 10, 64)
	if err != nil {
		fmt.Println("Invalid Delay value. Must be an integer.")
		return
	}

	// Construct the HTTP request
	resp, err := grpcClient.Delay(context.Background(), &rcppb.DelayRequest{NodeId: to, Delay: delay})
	if err != nil {
		log.Printf("Error setting delay: %v", err)
		return
	}

	fmt.Println("Delay request sent! Response: ", resp.Value)
}

func main() {
	config, err := LoadConfig("../nodes.json")
	if err != nil {
		log.Fatal("Error loading config: ", err)
	}

	// serverMap := MapServerIDToHTTPPort(config)
	grpcClientMap, err := establishConns(config)
	if err != nil {
		log.Fatal("Failed to establish connections to nodes: ", err)
	}
	
	for {
		displayMenu()
		fmt.Print("Enter choice: ")
		var choice int
		fmt.Scan(&choice)

		switch choice {
		case 1:
			sendStoreRequest(grpcClientMap)
		case 2:
			getValue(grpcClientMap)
		case 3:
			killServer(grpcClientMap)
		case 4:
			reviveServer(grpcClientMap)
		case 5:
			doPartition(grpcClientMap)
		case 6:
			sendDelayRequest(grpcClientMap)
		case 7:
			sendDeleteRequest(grpcClientMap)
			// getBalance(grpcClientMap)
		// case 8:
		// 	depositChecking(grpcClientMap)
		// case 9:
		// 	writeCheck(grpcClientMap)
		case 10:
			printMetric()
		case 0:
			readAndSend("ops.json", grpcClientMap)
		case 12:
			printTP()
		case -1:
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Invalid choice, try again.")
		}
	}
}
