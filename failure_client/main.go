package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	ID   string `json:"id"`
	Port string `json:"http_port"`
	IP   string `json:"ip"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type CauseFailureResponse struct {
	Success bool `json:"success"`
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

	contactNode      string
	nodeToAddressMap map[string]string
	killedNodes      map[string]struct{}
	failures         FailureList
	config           Config
)

func main() {
	flag.Var(&failures, "failures", "List of failures in time:type format, e.g., 5:leader,10:non-leader,15:random,20:revive")
	flag.Parse()

	nodeToAddressMap = make(map[string]string)
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
		nodeToAddressMap[node.ID] = fmt.Sprintf("%s%s", node.IP, node.Port)
		if contactNode == "" {
			contactNode = node.ID
		}
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
	for nodeId := range nodeToAddressMap {
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

		res, err := http.Get(fmt.Sprintf("http://%s/cause-failure?type=%s", nodeToAddressMap[contactServer], failureType))
		if err != nil {
			log.Panicf("Unexpected error trying to cause failure %s at %d seconds on %s: %v", failureType, waitInSeconds, contactServer, err)
		}

		defer res.Body.Close()

		// Redirected to a leader
		if res.StatusCode == http.StatusTemporaryRedirect {
			var data ErrorResponse
			json.NewDecoder(res.Body).Decode(&data)
			contactServer = data.Error
			continue
		}

		if res.StatusCode == http.StatusOK {
			var data CauseFailureResponse
			json.NewDecoder(res.Body).Decode(&data)
			if data.Success {
				log.Printf("Caused failure %s at %d seconds on %s", failureType, waitInSeconds, contactServer)

				if failureType == "revive" {
					delete(killedNodes, contactServer)
				} else {
					killedNodes[contactServer] = struct{}{}
				}

				return
			} else {
				// Try random node
				log.Printf("Tried to cause failure %s at %d seconds on %s", failureType, waitInSeconds, contactServer)
				contactServer = ""
				continue
			}
		}

		log.Panicf("Unexpected status code trying to cause failure %s at %d seconds on %s: %d", failureType, waitInSeconds, contactServer, res.StatusCode)
	}
}

// type Node struct {
// 	ID       string `json:"id"`
// 	HttpPort string `json:"http_port"`
// 	IP       string `json:"ip"`
// }

// type Config struct {
// 	Nodes []*Node `json:"nodes"`
// }

// type rcpDB struct {
// 	httpClient           http.Client
// 	serverAddr           string
// 	fieldcount           int64
// 	nodes                []*Node
// 	lastContactServerIdx int
// }

// type GetValueResponse struct {
// 	Found bool   `json:"found"`
// 	Value string `json:"value"`
// }

// type ErrorResponse struct {
// 	Error string `json:"error"`
// }

// type rcpCreator struct{}

// func init() {
// 	ycsb.RegisterDBCreator("rcp", rcpCreator{})
// }

// func (c rcpCreator) Create(p *properties.Properties) (ycsb.DB, error) {
// 	rcp := &rcpDB{}
// 	configStr, ok := p.Get("rcp.config")
// 	if !ok {
// 		return nil, fmt.Errorf("property 'rcp.config' must be specified")
// 	}
// 	var config Config
// 	err := json.Unmarshal([]byte(configStr), &config)
// 	if err != nil {
// 		return nil, fmt.Errorf("error unmarshalling config: %v", err)
// 	}

// 	rcp.nodes = config.Nodes
// 	client := http.Client{
// 		Timeout: 3 * time.Second,
// 	}

// 	rcp.changeContactServer("")

// 	rcp.httpClient = client
// 	rcp.fieldcount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
// 	return rcp, nil
// }

// func (db *rcpDB) changeContactServer(contactServer string) {
// 	var newContactServer *Node

// 	if contactServer == "" {
// 		db.lastContactServerIdx = (db.lastContactServerIdx + 1) % len(db.nodes)
// 		newContactServer = db.nodes[db.lastContactServerIdx]
// 	} else {
// 		for _, node := range db.nodes {
// 			if node.ID == contactServer {
// 				newContactServer = node
// 				break
// 			}
// 		}

// 		if newContactServer == nil {
// 			panic("Bad contact server given " + contactServer)
// 		}
// 	}

// 	db.serverAddr = "http://" + newContactServer.IP + newContactServer.HttpPort
// 	fmt.Printf("Setting contact address: %s\n", db.serverAddr)
// }

// func (db *rcpDB) Delete(ctx context.Context, table string, key string) error {
// 	reqURL := fmt.Sprintf("%s/del?key=%s", db.serverAddr, key)

// 	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
// 	if err != nil {
// 		return err
// 	}

// 	res, err := db.httpClient.Do(req)
// 	if err != nil {
// 		return err
// 	}
// 	defer res.Body.Close()

// 	if res.StatusCode == http.StatusOK {
// 		return nil
// 	}

// 	// Redirected to a new leader
// 	if res.StatusCode == http.StatusTemporaryRedirect {
// 		var data ErrorResponse
// 		json.NewDecoder(res.Body).Decode(&data)
// 		db.changeContactServer(data.Error)
// 		return db.Delete(ctx, table, key)
// 	}

// 	// Retry with other server
// 	if res.StatusCode == http.StatusExpectationFailed {
// 		db.changeContactServer("")
// 		return db.Delete(ctx, table, key)
// 	}

// 	return fmt.Errorf("bad status code: %d", res.StatusCode)
// }

// func (db *rcpDB) CleanupThread(ctx context.Context) {}

// func (db *rcpDB) Close() error {
// 	return nil
// }

// func (db *rcpDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
// 	return ctx
// }

// func (db *rcpDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

// 	valueStringMap := make(map[string]string)

// 	for k, v := range values {
// 		valueStringMap[k] = string(v)
// 	}

// 	return db.insertStringStringMap(ctx, key, table, valueStringMap)

// }

// func (db *rcpDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

// 	stringStringMap, err := db.getFieldValueMap(ctx, key, table)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read value: %v", err)
// 	}

// 	data := make(map[string][]byte)
// 	for k, v := range stringStringMap {
// 		data[k] = []byte(v)
// 	}

// 	return data, err
// }

// func (db *rcpDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
// 	return nil, fmt.Errorf("scan is not supported")
// }

// func (db *rcpDB) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {

// 	fullUpdate := false
// 	if int64(len(values)) == db.fieldcount {
// 		fullUpdate = true
// 	}

// 	if fullUpdate {
// 		return db.Insert(ctx, table, key, values)
// 	} else {
// 		keyValueMap, err := db.getFieldValueMap(ctx, key, table)
// 		if err != nil {
// 			return err
// 		}
// 		for k, v := range values {
// 			keyValueMap[k] = string(v)
// 		}
// 		return db.insertStringStringMap(ctx, key, table, keyValueMap)
// 	}
// }

// func (db *rcpDB) getFieldValueMap(ctx context.Context, key, table string) (map[string]string, error) {
// 	baseURL := fmt.Sprintf("%s/get", db.serverAddr)

// 	params := url.Values{}
// 	params.Add("key", key)
// 	params.Add("bucket", table)

// 	reqURL := baseURL + "?" + params.Encode()

// 	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp, err := db.httpClient.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer resp.Body.Close()

// 	// Retry with other server
// 	if resp.StatusCode == http.StatusExpectationFailed {
// 		db.changeContactServer("")
// 		return db.getFieldValueMap(ctx, key, table)
// 	}

// 	if resp.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
// 	}

// 	var getValResp GetValueResponse
// 	if err := json.NewDecoder(resp.Body).Decode(&getValResp); err != nil {
// 		return nil, fmt.Errorf("failed to decode json response: %w", err)
// 	}

// 	if !getValResp.Found {
// 		return nil, fmt.Errorf("key %s not found", key)
// 	}

// 	var stringStringMap map[string]string
// 	if err := json.Unmarshal([]byte(getValResp.Value), &stringStringMap); err != nil {
// 		return nil, fmt.Errorf("failed to decode inner JSON: %v", err)
// 	}

// 	return stringStringMap, nil
// }

// func (db *rcpDB) insertStringStringMap(ctx context.Context, key string, table string, valueStringMap map[string]string) error {
// 	baseURL := fmt.Sprintf("%s/put", db.serverAddr)
// 	valueBytes, err := json.Marshal(valueStringMap)
// 	if err != nil {
// 		return err
// 	}

// 	params := url.Values{}
// 	params.Add("key", key)
// 	params.Add("value", string(valueBytes))
// 	params.Add("bucket", table)

// 	reqURL := baseURL + "?" + params.Encode()

// 	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, nil)
// 	if err != nil {
// 		return err
// 	}

// 	res, err := db.httpClient.Do(req)
// 	if err != nil {
// 		return err
// 	}
// 	defer res.Body.Close()

// 	if res.StatusCode == http.StatusOK {
// 		return nil
// 	}

// 	// Redirected to a new leader
// 	if res.StatusCode == http.StatusTemporaryRedirect {
// 		var data ErrorResponse
// 		json.NewDecoder(res.Body).Decode(&data)
// 		db.changeContactServer(data.Error)
// 		return db.insertStringStringMap(ctx, key, table, valueStringMap)
// 	}

// 	// Retry with other server
// 	if res.StatusCode == http.StatusExpectationFailed {
// 		db.changeContactServer("")
// 		return db.insertStringStringMap(ctx, key, table, valueStringMap)
// 	}

// 	return fmt.Errorf("bad status code: %d", res.StatusCode)
// }
