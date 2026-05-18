package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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

type ReconfigResult struct {
	Reconfig     ReconfigEntity
	Success      bool
	Attempts     int
	ContactNode  string
	Latency      time.Duration
	ErrorMessage string
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
	resultCh := make(chan ReconfigResult, len(reconfigs))

	for _, reconfig := range reconfigs {
		wg.Add(1)
		go runReconfigurationAt(reconfig, &wg, resultCh)
	}

	wg.Wait()
	close(resultCh)

	results := make([]ReconfigResult, 0, len(reconfigs))
	for result := range resultCh {
		results = append(results, result)
	}

	reportLatencyStats(results)

	for _, result := range results {
		if !result.Success {
			os.Exit(1)
		}
	}
}

func runReconfigurationAt(reconfig ReconfigEntity, wg *sync.WaitGroup, resultCh chan<- ReconfigResult) {
	defer wg.Done()

	log.Printf("Scheduling reconfiguration at %d seconds to voters=%v", reconfig.Time, reconfig.VoterIDs)
	time.Sleep(time.Duration(reconfig.Time) * time.Second)
	begin := time.Now()

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
			latency := time.Since(begin)
			log.Printf("Reconfiguration at t=%d succeeded via %s in %v after %d attempts: %s", reconfig.Time, contactServer, latency, attempt, res.Value)
			resultCh <- ReconfigResult{
				Reconfig:    reconfig,
				Success:     true,
				Attempts:    attempt,
				ContactNode: contactServer,
				Latency:     latency,
			}
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
			latency := time.Since(begin)
			msg := fmt.Sprintf("rejected permanently by %s: error=%s message=%s", contactServer, res.Error.String(), res.Value)
			log.Printf("Reconfiguration at t=%d failed in %v after %d attempts: %s", reconfig.Time, latency, attempt, msg)
			resultCh <- ReconfigResult{
				Reconfig:     reconfig,
				Success:      false,
				Attempts:     attempt,
				ContactNode:  contactServer,
				Latency:      latency,
				ErrorMessage: msg,
			}
			return
		default:
			log.Printf("Attempt %d/%d reconfiguration at t=%d rejected by %s: error=%s message=%s", attempt, maxTries, reconfig.Time, contactServer, res.Error.String(), res.Value)
			contactServer = contactNode
		}

		time.Sleep(delay)
	}

	latency := time.Since(begin)
	msg := fmt.Sprintf("failed after %d attempts", maxTries)
	log.Printf("Reconfiguration at t=%d failed in %v: %s", reconfig.Time, latency, msg)
	resultCh <- ReconfigResult{
		Reconfig:     reconfig,
		Success:      false,
		Attempts:     maxTries,
		ContactNode:  contactServer,
		Latency:      latency,
		ErrorMessage: msg,
	}
}

func reportLatencyStats(results []ReconfigResult) {
	if len(results) == 0 {
		log.Printf("No reconfiguration events were executed.")
		return
	}

	slices.SortFunc(results, func(a, b ReconfigResult) int {
		switch {
		case a.Reconfig.Time < b.Reconfig.Time:
			return -1
		case a.Reconfig.Time > b.Reconfig.Time:
			return 1
		default:
			return strings.Compare(strings.Join(a.Reconfig.VoterIDs, "|"), strings.Join(b.Reconfig.VoterIDs, "|"))
		}
	})

	log.Printf("=== Reconfiguration Latency Report ===")

	successLatencies := make([]time.Duration, 0, len(results))
	failures := 0

	for _, result := range results {
		if result.Success {
			successLatencies = append(successLatencies, result.Latency)
			log.Printf("t=%ds voters=%v status=SUCCESS latency=%v attempts=%d contact=%s",
				result.Reconfig.Time, result.Reconfig.VoterIDs, result.Latency, result.Attempts, result.ContactNode)
		} else {
			failures++
			log.Printf("t=%ds voters=%v status=FAILED latency=%v attempts=%d contact=%s error=%s",
				result.Reconfig.Time, result.Reconfig.VoterIDs, result.Latency, result.Attempts, result.ContactNode, result.ErrorMessage)
		}
	}

	log.Printf("=== Reconfiguration Latency Summary ===")
	log.Printf("total=%d success=%d failed=%d", len(results), len(successLatencies), failures)

	if len(successLatencies) == 0 {
		log.Printf("No successful reconfiguration events; latency percentiles unavailable.")
		return
	}

	slices.SortFunc(successLatencies, func(a, b time.Duration) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})

	minLatency := successLatencies[0]
	maxLatency := successLatencies[len(successLatencies)-1]
	var sum time.Duration
	for _, latency := range successLatencies {
		sum += latency
	}
	avgLatency := sum / time.Duration(len(successLatencies))

	p50 := percentile(successLatencies, 50)
	p90 := percentile(successLatencies, 90)
	p95 := percentile(successLatencies, 95)
	p99 := percentile(successLatencies, 99)

	log.Printf("min=%v max=%v avg=%v p50=%v p90=%v p95=%v p99=%v",
		minLatency, maxLatency, avgLatency, p50, p90, p95, p99)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}

	// Nearest-rank percentile.
	rank := int(math.Ceil((p / 100) * float64(len(sorted))))
	if rank <= 0 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}
