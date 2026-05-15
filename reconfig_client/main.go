package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"rcp/rcppb"
	"slices"
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

type ReconfigEntity struct {
	Time     int64
	VoterIDs []string
}

type ReconfigList []ReconfigEntity

func (r *ReconfigList) String() string {
	return fmt.Sprint(*r)
}

// Expected format:
// --reconfigs "5:A|B|C,15:A|C|D"
func (r *ReconfigList) Set(value string) error {
	if value == "" {
		return nil
	}

	entries := strings.Split(value, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid reconfig format %q; expected time:v1|v2|v3", entry)
		}

		timeVal, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid time value %q: %w", parts[0], err)
		}
		if timeVal < 0 {
			return fmt.Errorf("invalid time value %q: must be >= 0", parts[0])
		}

		rawVoters := strings.Split(parts[1], "|")
		seen := make(map[string]struct{}, len(rawVoters))
		voterIDs := make([]string, 0, len(rawVoters))
		for _, raw := range rawVoters {
			id := strings.TrimSpace(raw)
			if id == "" {
				return fmt.Errorf("invalid voter list %q: contains empty node id", parts[1])
			}
			if _, exists := seen[id]; exists {
				return fmt.Errorf("invalid voter list %q: duplicate node id %s", parts[1], id)
			}
			seen[id] = struct{}{}
			voterIDs = append(voterIDs, id)
		}

		if len(voterIDs) == 0 {
			return fmt.Errorf("invalid voter list %q: empty target set", parts[1])
		}

		*r = append(*r, ReconfigEntity{
			Time:     timeVal,
			VoterIDs: voterIDs,
		})
	}

	slices.SortFunc(*r, func(a, b ReconfigEntity) int {
		switch {
		case a.Time < b.Time:
			return -1
		case a.Time > b.Time:
			return 1
		default:
			return 0
		}
	})

	return nil
}

var (
	configFile = flag.String("config-file", "./nodes.json", "node config JSON filename")
	retries    = flag.Int("retries", 100, "maximum retries per reconfiguration event")
	retryDelay = flag.Int("retry-delay-ms", 250, "retry delay in milliseconds")

	reconfigs     ReconfigList
	grpcClientMap map[string]rcppb.RCPClient
	knownNodeSet  map[string]struct{}
	config        Config
	contactNode   string
)

func main() {
	flag.Var(&reconfigs, "reconfigs", "List of reconfigurations in time:v1|v2|v3 format, e.g., 5:A|B|C,15:A|C|D")
	flag.Parse()

	if len(reconfigs) == 0 {
		log.Fatalf("missing --reconfigs")
	}

	grpcClientMap = make(map[string]rcppb.RCPClient)
	knownNodeSet = make(map[string]struct{})

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
		knownNodeSet[node.ID] = struct{}{}
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

	for _, reconfig := range reconfigs {
		for _, voterID := range reconfig.VoterIDs {
			if _, exists := knownNodeSet[voterID]; !exists {
				log.Fatalf("invalid reconfiguration at t=%d: unknown node id %s", reconfig.Time, voterID)
			}
		}
	}

	var wg sync.WaitGroup
	for _, reconfig := range reconfigs {
		wg.Add(1)
		go runReconfigurationAt(reconfig, &wg)
	}

	wg.Wait()
}

func runReconfigurationAt(reconfig ReconfigEntity, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("Scheduling reconfiguration at %d seconds to voters=%v", reconfig.Time, reconfig.VoterIDs)
	time.Sleep(time.Duration(reconfig.Time) * time.Second)

	contactServer := contactNode
	req := &rcppb.ReconfigureRequest{VoterIds: reconfig.VoterIDs}

	maxTries := *retries
	if maxTries <= 0 {
		maxTries = 1
	}
	delay := time.Duration(*retryDelay) * time.Millisecond
	if delay < 0 {
		delay = 0
	}

	for attempt := 1; attempt <= maxTries; attempt++ {
		client, exists := grpcClientMap[contactServer]
		if !exists {
			log.Printf("Unknown contact node %s; reset to default contact node %s", contactServer, contactNode)
			contactServer = contactNode
			time.Sleep(delay)
			continue
		}

		res, err := client.Reconfigure(context.Background(), req)
		if err != nil {
			log.Printf("Attempt %d/%d failed to call Reconfigure via %s at t=%d: %v", attempt, maxTries, contactServer, reconfig.Time, err)
			time.Sleep(delay)
			continue
		}

		if res.Success {
			log.Printf("Reconfiguration at t=%d succeeded via %s: %s", reconfig.Time, contactServer, res.Value)
			return
		}

		switch res.Error {
		case rcppb.ErrorType_NOT_LEADER:
			if res.Value != "" {
				log.Printf("Attempt %d/%d redirected from %s to leader %s", attempt, maxTries, contactServer, res.Value)
				contactServer = res.Value
			} else {
				log.Printf("Attempt %d/%d got NOT_LEADER from %s without leader hint", attempt, maxTries, contactServer)
				contactServer = contactNode
			}
		case rcppb.ErrorType_BAD_REQUEST, rcppb.ErrorType_NOT_SUPPORTED:
			log.Fatalf("Reconfiguration at t=%d rejected permanently by %s: error=%s message=%s", reconfig.Time, contactServer, res.Error.String(), res.Value)
		default:
			log.Printf("Attempt %d/%d reconfiguration at t=%d rejected by %s: error=%s message=%s", attempt, maxTries, reconfig.Time, contactServer, res.Error.String(), res.Value)
			contactServer = contactNode
		}

		time.Sleep(delay)
	}

	log.Fatalf("Reconfiguration at t=%d failed after %d attempts", reconfig.Time, maxTries)
}
