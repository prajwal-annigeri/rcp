package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Node struct {
	ID   string `json:"id"`
	Port string `json:"port"`
	IP   string `json:"ip"`
}

type Config struct {
	Nodes []Node `json:"nodes"`
}

type RCPClient struct {
	kvClients   map[string]kvpb.KVStoreClient
	orcaClients map[string]orcapb.OrcaClient
	conns       map[string]*grpc.ClientConn
}

func main() {
	config, err := LoadConfig("../nodes.json")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	client := newRCPClient()
	defer client.close()

	for _, node := range config.Nodes {
		addr := fmt.Sprintf("%s:%s", node.IP, node.Port)
		if err := client.addNode(node.ID, addr); err != nil {
			log.Fatalf("Failed to connect to %s: %v", node.ID, err)
		}
	}

	for {
		fmt.Println("\nMenu:")
		fmt.Println("1. Set value")
		fmt.Println("2. Get value")
		fmt.Println("3. Delete key")
		fmt.Println("4. Cause failure")
		fmt.Println("0. Exit")
		fmt.Print("Enter choice: ")

		var choice int
		fmt.Scan(&choice)

		switch choice {
		case 1:
			client.setValue()
		case 2:
			client.getValue()
		case 3:
			client.deleteKey()
		case 4:
			client.causeFailure()
		case 0:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid choice.")
		}
	}
}

func newRCPClient() *RCPClient {
	return &RCPClient{
		kvClients:   make(map[string]kvpb.KVStoreClient),
		orcaClients: make(map[string]orcapb.OrcaClient),
		conns:       make(map[string]*grpc.ClientConn),
	}
}

func (c *RCPClient) addNode(id, addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	c.conns[id] = conn
	c.kvClients[id] = kvpb.NewKVStoreClient(conn)
	c.orcaClients[id] = orcapb.NewOrcaClient(conn)
	return nil
}

func (c *RCPClient) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *RCPClient) promptServerID() (string, bool) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Node ID (e.g., S1): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)
	_, ok := c.kvClients[serverID]
	return serverID, ok
}

func (c *RCPClient) setValue() {
	serverID, ok := c.promptServerID()
	if !ok {
		fmt.Println("Invalid server ID.")
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Value: ")
	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.kvClients[serverID].PerformOperation(ctx, &kvpb.KVRequest{
		Op:     kvpb.OperationType_STORE,
		Key:    key,
		Value:  value,
		Bucket: bucket,
	})
	if err != nil {
		c.handleError(err)
		return
	}

	fmt.Println("Store request sent!")
}

func (c *RCPClient) getValue() {
	serverID, ok := c.promptServerID()
	if !ok {
		fmt.Println("Invalid server ID.")
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.kvClients[serverID].PerformOperation(ctx, &kvpb.KVRequest{
		Op:     kvpb.OperationType_GET,
		Key:    key,
		Bucket: bucket,
	})
	if err != nil {
		c.handleError(err)
		return
	}

	if !resp.GetSuccess() {
		fmt.Printf("Error: %s\n", resp.GetError())
		return
	}

	fmt.Printf("Value: %s\n", resp.GetValue())
}

func (c *RCPClient) deleteKey() {
	serverID, ok := c.promptServerID()
	if !ok {
		fmt.Println("Invalid server ID.")
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.kvClients[serverID].PerformOperation(ctx, &kvpb.KVRequest{
		Op:     kvpb.OperationType_DELETE,
		Key:    key,
		Bucket: bucket,
	})
	if err != nil {
		c.handleError(err)
		return
	}

	fmt.Println("Delete request sent!")
}

func (c *RCPClient) causeFailure() {
	serverID, ok := c.promptServerID()
	if !ok {
		fmt.Println("Invalid server ID.")
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter failure type (leader/non-leader/random/revive): ")
	failureType, _ := reader.ReadString('\n')
	failureType = strings.TrimSpace(failureType)

	ft, ok := map[string]orcapb.FailureType{
		"leader":     orcapb.FailureType_LEADER,
		"non-leader": orcapb.FailureType_REPLICA,
		"random":     orcapb.FailureType_RANDOM,
		"revive":     orcapb.FailureType_REVIVE,
	}[failureType]
	if !ok {
		fmt.Println("Unsupported failure type.")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := c.orcaClients[serverID].CauseFailure(ctx, &orcapb.CauseFailureRequest{
		Type: ft,
	})
	if err != nil {
		c.handleError(err)
		return
	}

	fmt.Println("Failure request sent!")
}

func (c *RCPClient) handleError(err error) {
	st, ok := status.FromError(err)
	if !ok {
		fmt.Printf("RPC error: %v\n", err)
		return
	}

	if st.Code() == codes.PermissionDenied {
		fmt.Printf("Redirected to leader: %s\n", st.Message())
		return
	}

	fmt.Printf("RPC error: %v\n", err)
}

func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
