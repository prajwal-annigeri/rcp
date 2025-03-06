package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	fmt.Println("1. Send Store Request")
	fmt.Println("2. Get Value")
	fmt.Println("3. Kill server")
	fmt.Println("4. Revive server")
	fmt.Println("5. Exit")
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
func sendStoreRequest(serverMap map[string]string) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	httpPort, exists := serverMap[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	// Ask for Key and Value
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Value: ")
	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	// Construct the HTTP request
	url := fmt.Sprintf("http://localhost%s/put?key=%s&value=%s", httpPort, url.QueryEscape(key), url.QueryEscape(value))
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Store request sent! Response:", resp.Status)
}

type GetValueResponse struct {
	Value string `json:"value"`
}

// GetValue makes HTTP request to query KV store
func GetValue(serverMap map[string]string) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3) or 'all' to query all servers: ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)


	// Ask for Key
	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	if serverID == "all" {
		GetValuesFromAll(serverMap, key)
	} else {
		// Get HTTP port for the server
		httpPort, exists := serverMap[serverID]
		if !exists {
			fmt.Println("Invalid Server ID!")
			return
		}
		value, err := GetValueFrom(httpPort, key)
		if err != nil {
			fmt.Printf("Error getting value from server %s: %v\n", serverID, err)
			return
		}
		fmt.Printf("Value on %s: %s\n", serverID, value)
	}

}

func GetValueFrom(httpPort, key string) (string, error) {
	url := fmt.Sprintf("http://localhost%s/get?key=%s", httpPort, url.QueryEscape(key))
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var respStruct GetValueResponse
	err = json.Unmarshal(body, &respStruct)
	if err != nil {
		return "", err
	}

	return respStruct.Value, nil
}

// GetValuesFromAll queries all servers for a key
func GetValuesFromAll(serverMap map[string]string, key string) {
	for serverID, httpPort := range serverMap {
		value, err := GetValueFrom(httpPort, key)
		if err != nil {
			fmt.Printf("Error querying server %s: %v\n", serverID, err)
		} else {
			fmt.Printf("%s: %s\n", serverID, value)
		}
	}
}

func killServer(serverMap map[string]string) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	httpPort, exists := serverMap[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	// Construct the HTTP request
	url := fmt.Sprintf("http://localhost%s/kill", httpPort)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()
}

func reviveServer(serverMap map[string]string) {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	httpPort, exists := serverMap[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	// Construct the HTTP request
	url := fmt.Sprintf("http://localhost%s/revive", httpPort)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()
}

func main() {
	config, err := LoadConfig("../nodes.json")
	if err != nil {
		log.Fatal("Error loading config:", err)
	}

	serverMap := MapServerIDToHTTPPort(config)

	for {
		displayMenu()
		fmt.Print("Enter choice: ")
		var choice int
		fmt.Scan(&choice)

		switch choice {
		case 1:
			sendStoreRequest(serverMap)
		case 2:
			GetValue(serverMap)
		case 3:
			killServer(serverMap)
		case 4:
			reviveServer(serverMap)
		case 5:
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Invalid choice, try again.")
		}
	}
}
