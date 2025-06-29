package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Config struct {
	Nodes    []Node          `json:"nodes"`
	Failures []FailureEntity `json:"failures"`
}

type Node struct {
	ID   string `json:"id"`
	Port string `json:"http_port"`
	IP   string `json:"ip"`
}

type FailureEntity struct {
	Time int64  `json:"time"`
	Type string `json:"type"`
}

var (
	contactNode      string
	nodeToAddressMap sync.Map
	config           Config
)

func main() {
	configJSON, err := os.Open("./failure_config.json")
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}
	defer configJSON.Close()

	configBytes, err := io.ReadAll(configJSON)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling config: %v", err)
	}

	for _, node := range config.Nodes {
		nodeToAddressMap.Store(node.ID, fmt.Sprintf("%s%s", node.IP, node.Port))
		if contactNode == "" {
			contactNode = node.ID
		}
	}
	var wg sync.WaitGroup
	for _, failure := range config.Failures {
		wg.Add(1)
		go causeFailure(failure.Type, failure.Time, &wg)
	}

	wg.Wait()
}

func changeContactNode() {
	for _, node := range config.Nodes {
		if contactNode != node.ID {
			contactNode = node.ID
			return
		}
	}
}

func causeFailure(failureType string, waitInSeconds int64, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Scheduling %s failure after %d seconds", failureType, waitInSeconds)
	time.Sleep(time.Duration(waitInSeconds) * time.Second)
	for {
		address, _ := nodeToAddressMap.Load(contactNode)
		resp, err := http.Get(fmt.Sprintf("http://%s/cause-failure?type=%s", address, failureType))
		if err != nil {
			log.Printf("Error causing failure of type %s (%d seconds): %v", failureType, waitInSeconds, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read response body: %s", err)
		}

		if resp.StatusCode == http.StatusOK {
			log.Printf("Caused failure: %s (%d seconds). Response: %v", failureType, waitInSeconds, string(body))
			return
		} else {
			log.Printf("Error response %d. Changing contact node", resp.StatusCode)
			changeContactNode()
		}
	}
}
