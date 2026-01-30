package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"
	"rcp/node"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

var (
	nodeId            = flag.String("id", "", "Node ID")
	logs              = flag.Bool("logs", false, "Logging")
	runtimeConfigFile = flag.String("runtime-config", "./runtime.conf", "runtime config filename")
	configFlag        = flag.String("config", "", "node config JSON")
	configFile        = flag.String("config-file", "./nodes.json", "node config JSON filename")
)

type runtimeConfig struct {
	Protocol           string
	Persistent         bool
	K                  int
	BatchSize          int
	BackoffDec         int
	ConsensusTimeout   int
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatTimeout   int
}

func loadRuntimeConfig(path string) (runtimeConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return runtimeConfig{}, err
	}
	defer file.Close()

	cfg := runtimeConfig{}
	seen := map[string]bool{}
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return runtimeConfig{}, fmt.Errorf("invalid runtime config line %d: %q", lineNo, line)
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch key {
		case "protocol":
			cfg.Protocol = value
			seen[key] = true
		case "persistent":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid persistent value on line %d: %w", lineNo, err)
			}
			cfg.Persistent = parsed
			seen[key] = true
		case "k":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid k value on line %d: %w", lineNo, err)
			}
			cfg.K = parsed
			seen[key] = true
		case "batch_size":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid batch_size value on line %d: %w", lineNo, err)
			}
			cfg.BatchSize = parsed
			seen[key] = true
		case "backoff_decrement":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid backoff_decrement value on line %d: %w", lineNo, err)
			}
			cfg.BackoffDec = parsed
			seen[key] = true
		case "consensus_timeout_ms":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid consensus_timeout_ms value on line %d: %w", lineNo, err)
			}
			cfg.ConsensusTimeout = parsed
			seen[key] = true
		case "election_timeout_min_ms":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid election_timeout_min_ms value on line %d: %w", lineNo, err)
			}
			cfg.ElectionTimeoutMin = parsed
			seen[key] = true
		case "election_timeout_max_ms":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid election_timeout_max_ms value on line %d: %w", lineNo, err)
			}
			cfg.ElectionTimeoutMax = parsed
			seen[key] = true
		case "heartbeat_timeout_ms":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("invalid heartbeat_timeout_ms value on line %d: %w", lineNo, err)
			}
			cfg.HeartbeatTimeout = parsed
			seen[key] = true
		default:
			return runtimeConfig{}, fmt.Errorf("unknown runtime config key %q on line %d", key, lineNo)
		}
	}
	if err := scanner.Err(); err != nil {
		return runtimeConfig{}, err
	}

	requiredKeys := []string{
		"protocol",
		"persistent",
		"k",
		"batch_size",
		"backoff_decrement",
		"consensus_timeout_ms",
		"election_timeout_min_ms",
		"election_timeout_max_ms",
		"heartbeat_timeout_ms",
	}
	var missing []string
	for _, key := range requiredKeys {
		if !seen[key] {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return runtimeConfig{}, fmt.Errorf("missing runtime config keys: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func main() {
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lshortfile)

	if !*logs {
		log.SetOutput(io.Discard)
	}

	runtimeCfg, err := loadRuntimeConfig(*runtimeConfigFile)
	if err != nil {
		log.Fatalf("failed to load runtime config (%s): %v", *runtimeConfigFile, err)
	}

	nodeCfg := node.NodeConfig{
		NodeID:             *nodeId,
		Protocol:           runtimeCfg.Protocol,
		Persistent:         runtimeCfg.Persistent,
		ConfigJSON:         *configFlag,
		ConfigFile:         *configFile,
		K:                  runtimeCfg.K,
		BatchSize:          runtimeCfg.BatchSize,
		BackoffDec:         runtimeCfg.BackoffDec,
		ConsensusTimeout:   runtimeCfg.ConsensusTimeout,
		ElectionTimeoutMin: runtimeCfg.ElectionTimeoutMin,
		ElectionTimeoutMax: runtimeCfg.ElectionTimeoutMax,
		HeartbeatTimeout:   runtimeCfg.HeartbeatTimeout,
	}

	if err := nodeCfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	nodeInstance, err := node.NewNode(nodeCfg)
	if err != nil {
		log.Fatalf("Error creating node: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", nodeInstance.Port))
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", nodeInstance.Port, err)
	}
	log.Printf("Listening on port: %v\n", nodeInstance.Port)

	grpcSrv := grpc.NewServer()

	orcapb.RegisterOrcaServer(grpcSrv, nodeInstance)
	kvpb.RegisterKVStoreServer(grpcSrv, nodeInstance)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	if err := nodeInstance.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	nodeInstance.RunInteractiveMenu()
}
