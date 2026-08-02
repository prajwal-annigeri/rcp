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
	Reconfig      ReconfigEntity
	Success       bool
	Attempts      int
	ContactNode   string
	Latency       time.Duration
	ActiveLatency time.Duration
	RetryDelay    time.Duration
	ErrorMessage  string
}

type ReconfigList []ReconfigEntity

func (r *ReconfigList) String() string {
	return fmt.Sprint(*r)
}

func parseVoterIDs(raw string) ([]string, error) {
	rawVoters := strings.Split(raw, "|")
	seen := make(map[string]struct{}, len(rawVoters))
	voterIDs := make([]string, 0, len(rawVoters))
	for _, rawID := range rawVoters {
		id := strings.TrimSpace(rawID)
		if id == "" {
			return nil, fmt.Errorf("contains empty node id")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate node id %s", id)
		}
		seen[id] = struct{}{}
		voterIDs = append(voterIDs, id)
	}

	if len(voterIDs) == 0 {
		return nil, fmt.Errorf("empty target set")
	}

	return voterIDs, nil
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

		voterIDs, err := parseVoterIDs(parts[1])
		if err != nil {
			return fmt.Errorf("invalid voter list %q: %w", parts[1], err)
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

func generateReconfigsFromPattern(pattern string, intervalSec int64, totalTimeSec int64) (ReconfigList, error) {
	if intervalSec <= 0 {
		return nil, fmt.Errorf("invalid --reconfig-interval=%d; must be > 0", intervalSec)
	}
	if totalTimeSec <= 0 {
		return nil, fmt.Errorf("invalid --time=%d; must be > 0", totalTimeSec)
	}

	rawSets := strings.Split(pattern, ",")
	targets := make([][]string, 0, len(rawSets))
	for _, rawSet := range rawSets {
		trimmed := strings.TrimSpace(rawSet)
		if trimmed == "" {
			continue
		}
		voters, err := parseVoterIDs(trimmed)
		if err != nil {
			return nil, fmt.Errorf("invalid --pattern set %q: %w", trimmed, err)
		}
		targets = append(targets, voters)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("invalid --pattern: expected at least one voter set")
	}

	reconfigs := make(ReconfigList, 0)
	patternIdx := 0
	for t := intervalSec; t <= totalTimeSec; t += intervalSec {
		voters := append([]string(nil), targets[patternIdx%len(targets)]...)
		reconfigs = append(reconfigs, ReconfigEntity{
			Time:     t,
			VoterIDs: voters,
		})
		patternIdx++
	}

	if len(reconfigs) == 0 {
		return nil, fmt.Errorf("no reconfiguration events generated: ensure --time >= --reconfig-interval")
	}

	return reconfigs, nil
}

var (
	configFile         = flag.String("config-file", "./nodes.json", "node config JSON filename")
	retries            = flag.Int("retries", 100, "maximum retries per reconfiguration event")
	retryDelay         = flag.Int("retry-delay-ms", 250, "retry delay in milliseconds")
	connectTimeoutMS   = flag.Int("connect-timeout-ms", 2000, "per-attempt Healthz timeout in milliseconds")
	connectRetryMS     = flag.Int("connect-retry-ms", 200, "Healthz retry delay in milliseconds")
	connectMaxWaitSec  = flag.Int("connect-max-wait-sec", 30, "maximum total wait per node connection in seconds")
	reconfigureTimeout = flag.Int("reconfigure-timeout-ms", 1500, "Reconfigure RPC timeout in milliseconds")
	pattern            = flag.String("pattern", "", "Comma-separated voter sets, e.g., A|B|C,A|B|C|D|E|F|G")
	reconfigInterval   = flag.Int("reconfig-interval", 0, "Schedule interval in seconds when using --pattern")
	totalTime          = flag.Int("time", 0, "Total schedule time in seconds when using --pattern")

	reconfigs     ReconfigList
	grpcClientMap map[string]rcppb.RCPClient
	knownNodeSet  map[string]struct{}
	config        Config
)

func main() {
	flag.Var(&reconfigs, "reconfigs", "List of reconfigurations in time:v1|v2|v3 format, e.g., 5:A|B|C,15:A|C|D")
	flag.Parse()

	patternScheduleSpecified := strings.TrimSpace(*pattern) != "" || *reconfigInterval != 0 || *totalTime != 0
	if len(reconfigs) > 0 && patternScheduleSpecified {
		log.Fatalf("invalid flags: use either --reconfigs or (--pattern + --reconfig-interval + --time), not both")
	}

	if len(reconfigs) == 0 {
		if strings.TrimSpace(*pattern) == "" {
			log.Fatalf("missing schedule: provide --reconfigs or --pattern with --reconfig-interval and --time")
		}
		generated, err := generateReconfigsFromPattern(*pattern, int64(*reconfigInterval), int64(*totalTime))
		if err != nil {
			log.Fatalf("failed to generate reconfiguration schedule: %v", err)
		}
		reconfigs = generated
		log.Printf("Generated %d reconfiguration events from pattern schedule (interval=%ds, time=%ds)", len(reconfigs), *reconfigInterval, *totalTime)
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
		addr := fmt.Sprintf("%s:%s", normalizeDialHost(node.IP), node.Port)

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to %s", node.ID)
		}

		client := rcppb.NewRCPClient(conn)
		grpcClientMap[node.ID] = client

		err = waitForHealthz(node.ID, grpcClientMap[node.ID])
		if err != nil {
			log.Fatalf("Failed to connect to %s: %v", node.ID, err)
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

	start := time.Now()
	prevVoters := make([]string, 0, len(config.Nodes))
	for _, node := range config.Nodes {
		prevVoters = append(prevVoters, node.ID)
	}
	lastSuccessfulLeader := ""
	results := make([]ReconfigResult, 0, len(reconfigs))
	for _, reconfig := range reconfigs {
		result := runReconfigurationAt(reconfig, prevVoters, lastSuccessfulLeader, start)
		results = append(results, result)
		if result.Success {
			prevVoters = append([]string(nil), reconfig.VoterIDs...)
			lastSuccessfulLeader = result.ContactNode
		}
	}

	reportLatencyStats(results)

	for _, result := range results {
		if !result.Success {
			os.Exit(1)
		}
	}
}

func normalizeDialHost(host string) string {
	if host == "localhost" {
		return "127.0.0.1"
	}
	return host
}

func waitForHealthz(nodeID string, client rcppb.RCPClient) error {
	perTryTimeout := time.Duration(*connectTimeoutMS) * time.Millisecond
	if perTryTimeout <= 0 {
		perTryTimeout = 2 * time.Second
	}

	retryDelayDur := time.Duration(*connectRetryMS) * time.Millisecond
	if retryDelayDur < 0 {
		retryDelayDur = 0
	}

	maxWait := time.Duration(*connectMaxWaitSec) * time.Second
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	deadline := time.Now().Add(maxWait)
	attempt := 0

	for {
		attempt++
		ctx, cancel := context.WithTimeout(context.Background(), perTryTimeout)
		_, err := client.Healthz(ctx, &rcppb.HealthzRequest{})
		cancel()
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("healthz timed out after %v (%d attempts): %w", maxWait, attempt, err)
		}

		if attempt == 1 || attempt%10 == 0 {
			log.Printf("Waiting for %s healthz (attempt=%d): %v", nodeID, attempt, err)
		}
		time.Sleep(retryDelayDur)
	}
}

func selectContactFromVoters(voters []string) string {
	for _, voterID := range voters {
		if _, exists := grpcClientMap[voterID]; exists {
			return voterID
		}
	}
	return ""
}

func selectNextContactFromVoters(voters []string, current string) string {
	if len(voters) == 0 {
		return ""
	}

	ordered := make([]string, 0, len(voters))
	for _, voterID := range voters {
		if _, exists := grpcClientMap[voterID]; exists {
			ordered = append(ordered, voterID)
		}
	}
	if len(ordered) == 0 {
		return ""
	}

	for i, voterID := range ordered {
		if voterID == current {
			return ordered[(i+1)%len(ordered)]
		}
	}

	return ordered[0]
}

func containsVoter(voters []string, nodeID string) bool {
	for _, voterID := range voters {
		if voterID == nodeID {
			return true
		}
	}
	return false
}

func runReconfigurationAt(reconfig ReconfigEntity, previousVoters []string, lastSuccessfulLeader string, start time.Time) ReconfigResult {
	log.Printf("Scheduling reconfiguration at %d seconds to voters=%v", reconfig.Time, reconfig.VoterIDs)
	scheduledAt := start.Add(time.Duration(reconfig.Time) * time.Second)
	if wait := time.Until(scheduledAt); wait > 0 {
		time.Sleep(wait)
	}
	begin := time.Now()
	activeLatency := time.Duration(0)

	contactServer := ""
	// Prefer the last successful leader if it is still a voter in the
	// previously successful configuration.
	if lastSuccessfulLeader != "" && containsVoter(previousVoters, lastSuccessfulLeader) {
		if _, exists := grpcClientMap[lastSuccessfulLeader]; exists {
			contactServer = lastSuccessfulLeader
		}
	}
	// Otherwise, start with voters from the new target configuration.
	if contactServer == "" {
		contactServer = selectContactFromVoters(reconfig.VoterIDs)
	}
	// Fallback to previous configuration voters if target voters are unavailable.
	if contactServer == "" {
		contactServer = selectContactFromVoters(previousVoters)
	}
	if contactServer == "" {
		for nodeID := range grpcClientMap {
			contactServer = nodeID
			break
		}
	}
	if contactServer == "" {
		return ReconfigResult{
			Reconfig:     reconfig,
			Success:      false,
			Attempts:     0,
			Latency:      time.Since(begin),
			ErrorMessage: "no available contact node",
		}
	}

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
		shouldDelay := true

		client, exists := grpcClientMap[contactServer]
		if !exists {
			log.Printf("Unknown contact node %s; selecting fallback contact", contactServer)
			contactServer = selectNextContactFromVoters(previousVoters, contactServer)
			if contactServer == "" {
				contactServer = selectNextContactFromVoters(reconfig.VoterIDs, "")
			}
			time.Sleep(delay)
			continue
		}

		reqTimeout := time.Duration(*reconfigureTimeout) * time.Millisecond
		if reqTimeout <= 0 {
			reqTimeout = 1500 * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
		attemptBegin := time.Now()
		res, err := client.Reconfigure(ctx, req)
		attemptLatency := time.Since(attemptBegin)
		activeLatency += attemptLatency
		cancel()
		if err != nil {
			log.Printf("Attempt %d/%d failed to call Reconfigure via %s at t=%d: %v", attempt, maxTries, contactServer, reconfig.Time, err)
			time.Sleep(delay)
			continue
		}

		if res.Success {
			latency := time.Since(begin)
			retryDelayTotal := latency - activeLatency
			if retryDelayTotal < 0 {
				retryDelayTotal = 0
			}
			log.Printf("Reconfiguration at t=%d succeeded via %s in %v (active=%v retry-delay=%v) after %d attempts: %s", reconfig.Time, contactServer, latency, activeLatency, retryDelayTotal, attempt, res.Value)
			return ReconfigResult{
				Reconfig:      reconfig,
				Success:       true,
				Attempts:      attempt,
				ContactNode:   contactServer,
				Latency:       latency,
				ActiveLatency: activeLatency,
				RetryDelay:    retryDelayTotal,
			}
		}

		switch res.Error {
		case rcppb.ErrorType_NOT_LEADER:
			if res.Value != "" && res.Value != contactServer {
				log.Printf("Attempt %d/%d redirected from %s to leader %s", attempt, maxTries, contactServer, res.Value)
				// Treat redirect as a new active request path; reset active latency
				// so "active" reflects attempts against the redirected leader.
				activeLatency = 0
				contactServer = res.Value
				shouldDelay = false
			} else {
				if res.Value == contactServer {
					log.Printf("Attempt %d/%d got NOT_LEADER from %s with same-node leader hint; delaying retry", attempt, maxTries, contactServer)
				} else {
					log.Printf("Attempt %d/%d got NOT_LEADER from %s without leader hint", attempt, maxTries, contactServer)
				}
				contactServer = selectNextContactFromVoters(previousVoters, contactServer)
				if contactServer == "" {
					contactServer = selectNextContactFromVoters(reconfig.VoterIDs, contactServer)
				}
				// Delay retries when leader location is uncertain or hint is not actionable.
				shouldDelay = true
			}
		case rcppb.ErrorType_BAD_REQUEST, rcppb.ErrorType_NOT_SUPPORTED:
			latency := time.Since(begin)
			retryDelayTotal := latency - activeLatency
			if retryDelayTotal < 0 {
				retryDelayTotal = 0
			}
			msg := fmt.Sprintf("rejected permanently by %s: error=%s message=%s", contactServer, res.Error.String(), res.Value)
			log.Printf("Reconfiguration at t=%d failed in %v (active=%v retry-delay=%v) after %d attempts: %s", reconfig.Time, latency, activeLatency, retryDelayTotal, attempt, msg)
			return ReconfigResult{
				Reconfig:      reconfig,
				Success:       false,
				Attempts:      attempt,
				ContactNode:   contactServer,
				Latency:       latency,
				ActiveLatency: activeLatency,
				RetryDelay:    retryDelayTotal,
				ErrorMessage:  msg,
			}
		default:
			log.Printf("Attempt %d/%d reconfiguration at t=%d rejected by %s: error=%s message=%s", attempt, maxTries, reconfig.Time, contactServer, res.Error.String(), res.Value)
			contactServer = selectNextContactFromVoters(previousVoters, contactServer)
			if contactServer == "" {
				contactServer = selectNextContactFromVoters(reconfig.VoterIDs, contactServer)
			}
		}

		if shouldDelay {
			time.Sleep(delay)
		}
	}

	latency := time.Since(begin)
	retryDelayTotal := latency - activeLatency
	if retryDelayTotal < 0 {
		retryDelayTotal = 0
	}
	msg := fmt.Sprintf("failed after %d attempts", maxTries)
	log.Printf("Reconfiguration at t=%d failed in %v (active=%v retry-delay=%v): %s", reconfig.Time, latency, activeLatency, retryDelayTotal, msg)
	return ReconfigResult{
		Reconfig:      reconfig,
		Success:       false,
		Attempts:      maxTries,
		ContactNode:   contactServer,
		Latency:       latency,
		ActiveLatency: activeLatency,
		RetryDelay:    retryDelayTotal,
		ErrorMessage:  msg,
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
	successActiveLatencies := make([]time.Duration, 0, len(results))
	failures := 0

	for _, result := range results {
		if result.Success {
			successLatencies = append(successLatencies, result.Latency)
			successActiveLatencies = append(successActiveLatencies, result.ActiveLatency)
			log.Printf("t=%ds voters=%v status=SUCCESS latency_total=%v latency_active=%v retry_delay=%v attempts=%d contact=%s",
				result.Reconfig.Time, result.Reconfig.VoterIDs, result.Latency, result.ActiveLatency, result.RetryDelay, result.Attempts, result.ContactNode)
		} else {
			failures++
			log.Printf("t=%ds voters=%v status=FAILED latency_total=%v latency_active=%v retry_delay=%v attempts=%d contact=%s error=%s",
				result.Reconfig.Time, result.Reconfig.VoterIDs, result.Latency, result.ActiveLatency, result.RetryDelay, result.Attempts, result.ContactNode, result.ErrorMessage)
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
	slices.SortFunc(successActiveLatencies, func(a, b time.Duration) int {
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
	p50Active := percentile(successActiveLatencies, 50)
	p90Active := percentile(successActiveLatencies, 90)
	p95Active := percentile(successActiveLatencies, 95)
	p99Active := percentile(successActiveLatencies, 99)

	log.Printf("total_latency: min=%v max=%v avg=%v p50=%v p90=%v p95=%v p99=%v",
		minLatency, maxLatency, avgLatency, p50, p90, p95, p99)
	log.Printf("active_latency: p50=%v p90=%v p95=%v p99=%v", p50Active, p90Active, p95Active, p99Active)
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
