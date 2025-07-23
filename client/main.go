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
	"time"
)

type Node struct {
	ID       string `json:"id"`
	HttpPort string `json:"http_port"`
	IP       string `json:"ip"`
}

type Config struct {
	K     int    `json:"K"`
	Nodes []Node `json:"nodes"`
}

type RCPClient struct {
	httpClient  http.Client
	serverAddrs map[string]string
}

type GetValueResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value"`
}

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

func main() {
	config, err := LoadConfig("../nodes.json")
	if err != nil {
		log.Fatal("Error loading config: ", err)
	}

	rcp := &RCPClient{}
	rcp.serverAddrs = make(map[string]string)

	for _, node := range config.Nodes {
		addr := "http://" + node.IP + node.HttpPort
		rcp.serverAddrs[node.ID] = addr
	}

	client := http.Client{
		Timeout: 10 * time.Second,
	}
	rcp.httpClient = client

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
			rcp.setValue()
		case 2:
			rcp.getValue()
		case 3:
			rcp.deleteKey()
		case 4:
			rcp.causeFailure()
		case 0:
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Invalid choice, try again.")
		}
	}
}

func (c *RCPClient) setValue() {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Node ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	addr, exists := c.serverAddrs[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

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

	params := url.Values{}
	params.Add("key", key)
	params.Add("value", value)
	params.Add("bucket", bucket)

	reqURL := fmt.Sprintf("%s/put", addr) + "?" + params.Encode()

	req, err := http.NewRequest(http.MethodPost, reqURL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("Time: %v", time.Since(begin))

	fmt.Println("Store request sent!")
}

func (c *RCPClient) deleteKey() {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Node ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	addr, exists := c.serverAddrs[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	begin := time.Now()

	params := url.Values{}
	params.Add("key", key)

	reqURL := fmt.Sprintf("%s/del", addr) + "?" + params.Encode()
	fmt.Println(reqURL)

	req, err := http.NewRequest(http.MethodDelete, reqURL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("Time: %v", time.Since(begin))

	fmt.Println("Delete request sent!")
}

// getValue makes Get request to query KV store
func (c *RCPClient) getValue() {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter Server ID (e.g., S1, S2, S3) or 'all' to query all servers: ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	fmt.Print("Enter Key: ")
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)

	fmt.Print("Enter Bucket: ")
	bucket, _ := reader.ReadString('\n')
	bucket = strings.TrimSpace(bucket)

	params := url.Values{}
	params.Add("key", key)
	params.Add("bucket", bucket)

	if serverID == "all" {
		for k, v := range c.serverAddrs {
			reqURL := fmt.Sprintf("%s/get", v) + "?" + params.Encode()

			req, err := http.NewRequest(http.MethodGet, reqURL, nil)
			if err != nil {
				fmt.Println("Error creating request:", err)
				continue
			}

			resp, err := c.httpClient.Do(req)
			if err != nil {
				fmt.Println("Error sending request:", err)
				continue
			}
			defer resp.Body.Close()

			var getValResp GetValueResponse
			if err := json.NewDecoder(resp.Body).Decode(&getValResp); err != nil {
				fmt.Println("Error decode json response:", err)
				continue
			}

			if !getValResp.Found {
				fmt.Println("Error key not found")
				continue
			}

			fmt.Printf("Value on %s: %s\n", k, getValResp.Value)
		}

	} else {
		// Get HTTP port for the server
		addr, exists := c.serverAddrs[serverID]
		if !exists {
			fmt.Println("Invalid Server ID!")
			return
		}

		reqURL := fmt.Sprintf("%s/get", addr) + "?" + params.Encode()

		req, err := http.NewRequest(http.MethodGet, reqURL, nil)
		if err != nil {
			fmt.Println("Error creating request:", err)
			return
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			fmt.Println("Error sending request:", err)
			return
		}
		defer resp.Body.Close()

		var getValResp GetValueResponse
		if err := json.NewDecoder(resp.Body).Decode(&getValResp); err != nil {
			fmt.Println("Error decode json response:", err)
			return
		}

		if !getValResp.Found {
			fmt.Println("Error key not found")
			return
		}

		fmt.Printf("Value on %s: %s\n", serverID, getValResp.Value)
	}

}

func (c *RCPClient) causeFailure() {
	reader := bufio.NewReader(os.Stdin)

	// Ask for Server ID
	fmt.Print("Enter contact server ID (e.g., S1, S2, S3): ")
	serverID, _ := reader.ReadString('\n')
	serverID = strings.TrimSpace(serverID)

	// Get HTTP port for the server
	addr, exists := c.serverAddrs[serverID]
	if !exists {
		fmt.Println("Invalid Server ID!")
		return
	}

	// Ask for Key and Value
	fmt.Print("Enter failure type (leader/non-leader/random/revive): ")
	failureType, _ := reader.ReadString('\n')
	failureType = strings.TrimSpace(failureType)

	resp, err := http.Get(fmt.Sprintf("%s/cause-failure?type=%s", addr, failureType))
	if err != nil {
		log.Printf("Error causing failure: %v", err)
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body: %s", err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		log.Printf("Cause failure response: %v", string(body))
		return
	} else {
		log.Printf("Unexpected error", resp.StatusCode)
		return
	}
}
