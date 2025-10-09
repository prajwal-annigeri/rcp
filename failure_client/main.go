package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"rcp/rcppb"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	ID   string `json:"id"`
	Port string `json:"port"`
	IP   string `json:"ip"`
}

type FailureEntity struct {
	Time int64
	Type string
}

type FailureList []FailureEntity

func (f *FailureList) String() string {
	return fmt.Sprint(*f)
}

func (f *FailureList) Set(value string) error {
	if value == "" {
		return nil
	}
	entries := strings.Split(value, ",")
	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid failure format: %s", entry)
		}
		timeVal, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid time value: %s", parts[0])
		}
		*f = append(*f, FailureEntity{
			Time: timeVal,
			Type: parts[1],
		})
	}
	return nil
}

var (
	configFile = flag.String("config-file", "./nodes.json", "node config JSON filename")

	contactNode   string
	grpcClientMap map[string]rcppb.RCPClient
	killedNodes   map[string]struct{}
	failures      FailureList
	config        Config
)

func main() {
	flag.Var(&failures, "failures", "List of failures in time:type format, e.g., 5:leader,10:non-leader,15:random,20:revive")
	flag.Parse()

	grpcClientMap = make(map[string]rcppb.RCPClient)
	killedNodes = map[string]struct{}{}

	configJSON, err := os.Open(*configFile)
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
		addr := fmt.Sprintf("%s:%s", node.IP, node.Port)
		if contactNode == "" {
			contactNode = node.ID
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to %s", node.ID)
		}

		client := rcppb.NewRCPClient(conn)
		grpcClientMap[node.ID] = client

		_, err = grpcClientMap[node.ID].Healthz(context.Background(), &rcppb.HealthzRequest{})
		if err != nil {
			log.Fatalf("Failed to connect to %s", node.ID)
		}
		log.Printf("Connected to %s!", node.ID)
	}

	var wg sync.WaitGroup
	for _, failure := range failures {
		wg.Add(1)
		go causeFailure(failure.Type, failure.Time, &wg)
	}

	wg.Wait()
}

func getRandomAliveNodeId() string {
	var nodeIds []string
	for nodeId := range grpcClientMap {
		if _, killed := killedNodes[nodeId]; !killed {
			nodeIds = append(nodeIds, nodeId)
		}
	}
	if len(nodeIds) > 0 {
		randomNodeId := nodeIds[rand.Intn(len(nodeIds))]
		return randomNodeId
	} else {
		log.Panic("all nodes have been killed")
		return ""
	}
}

func getRandomKilledNodeId() string {
	var nodeIds []string
	for nodeId := range killedNodes {
		nodeIds = append(nodeIds, nodeId)
	}
	if len(nodeIds) > 0 {
		randomNodeId := nodeIds[rand.Intn(len(nodeIds))]
		return randomNodeId
	} else {
		log.Panic("all nodes are alive")
		return ""
	}
}

func causeFailure(failureType string, waitInSeconds int64, wg *sync.WaitGroup) {
	defer wg.Done()

	switch failureType {
	case "revive":
		log.Printf("Scheduling revive after %d seconds", waitInSeconds)
	default:
		log.Printf("Scheduling %s failure after %d seconds", failureType, waitInSeconds)
	}

	time.Sleep(time.Duration(waitInSeconds) * time.Second)
	contactServer := ""

	for {
		if contactServer == "" {
			if failureType == "revive" {
				contactServer = getRandomKilledNodeId()
			} else {
				contactServer = getRandomAliveNodeId()
			}
		}

		var req *rcppb.CauseFailureRequest
		switch failureType {
		case "revive":
			req = &rcppb.CauseFailureRequest{
				Type: rcppb.FailureType_REVIVE,
			}
		case "random":
			req = &rcppb.CauseFailureRequest{
				Type: rcppb.FailureType_RANDOM,
			}
		case "leader":
			req = &rcppb.CauseFailureRequest{
				Type: rcppb.FailureType_LEADER,
			}
		case "non-leader":
			req = &rcppb.CauseFailureRequest{
				Type: rcppb.FailureType_REPLICA,
			}
		}

		client := grpcClientMap[contactServer]
		res, err := client.CauseFailure(context.Background(), req)

		if err != nil {
			log.Panicf("Unexpected error trying to cause failure %s at %d seconds on %s: %v", failureType, waitInSeconds, contactServer, err)
		}

		if res.Success {
			log.Printf("Caused failure %s at %d seconds on %s", failureType, waitInSeconds, contactServer)

			if failureType == "revive" {
				delete(killedNodes, contactServer)
			} else {
				killedNodes[contactServer] = struct{}{}
			}

			return
		} else {
			// Redirected to a leader
			if res.Error == rcppb.ErrorType_NOT_LEADER {
				contactServer = res.Value
				continue
			}

			// Try random node
			log.Printf("Tried to cause failure %s at %d seconds on %s", failureType, waitInSeconds, contactServer)
			contactServer = ""
			continue
		}
	}
}
