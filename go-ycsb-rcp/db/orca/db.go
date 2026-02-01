package rcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/grpc/rcppb"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Nodes []*Node `json:"nodes"`
}

type Node struct {
	ID   string `json:"id"`
	Port string `json:"port"`
	IP   string `json:"ip"`
}

type orcaDB struct {
	verbose              bool
	grpcClientMap        map[string]rcppb.RCPClient
	contactServer        string
	lastContactServerIdx int
	fieldcount           int64
	nodes                []*Node
}

type orcaCreator struct{}

func init() {
	ycsb.RegisterDBCreator("orca", orcaCreator{})
}

func (c orcaCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	orca := &orcaDB{}

	orca.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)

	configStr, ok := p.Get("orca.config")
	if !ok {
		return nil, fmt.Errorf("property 'orca.config' must be specified")
	}
	var config Config
	err := json.Unmarshal([]byte(configStr), &config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %v", err)
	}

	orca.nodes = config.Nodes
	orca.grpcClientMap = make(map[string]rcppb.RCPClient)

	for _, node := range config.Nodes {
		addr := fmt.Sprintf("%s:%s", node.IP, node.Port)

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to %s: %v", node.ID, err)
		}

		client := rcppb.NewRCPClient(conn)
		orca.grpcClientMap[node.ID] = client
	}

	orca.changeContactServer("")

	orca.fieldcount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	return orca, nil
}

func (db *orcaDB) getContactServerGrpcClient() rcppb.RCPClient {
	return db.grpcClientMap[db.contactServer]
}

func (db *orcaDB) changeContactServer(contactServer string) {
	if contactServer == "" {
		db.lastContactServerIdx = (db.lastContactServerIdx + 1) % len(db.nodes)
		db.contactServer = db.nodes[db.lastContactServerIdx].ID
	} else {
		if db.contactServer == contactServer {
			return
		}
		db.contactServer = contactServer
	}

	if db.verbose {
		fmt.Printf("Change contact server to %s", db.contactServer)
	}
}

func (db *orcaDB) Delete(ctx context.Context, table string, key string) error {
	req := &rcppb.DeleteRequest{Key: key, Bucket: table}

	client := db.getContactServerGrpcClient()
	res, err := client.Delete(ctx, req)

	if err != nil {
		return fmt.Errorf("unexpected error on delete: %v", err)
	}

	if res.Success {
		return nil
	}

	// Redirected to a new leader
	if res.Error == rcppb.ErrorType_NOT_LEADER {
		db.changeContactServer(res.Value)
		return db.Delete(ctx, table, key)
	}

	// Retry with other server
	db.changeContactServer("")
	return db.Delete(ctx, table, key)
}

func (db *orcaDB) CleanupThread(ctx context.Context) {}

func (db *orcaDB) Close() error {
	return nil
}

func (db *orcaDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (db *orcaDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	valueStringMap := make(map[string]string)

	for k, v := range values {
		valueStringMap[k] = string(v)
	}

	return db.insertStringStringMap(ctx, key, table, valueStringMap)

}

func (db *orcaDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

	stringStringMap, err := db.getFieldValueMap(ctx, key, table)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %v", err)
	}

	data := make(map[string][]byte)
	for k, v := range stringStringMap {
		data[k] = []byte(v)
	}

	return data, err
}

func (db *orcaDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *orcaDB) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {

	fullUpdate := false
	if int64(len(values)) == db.fieldcount {
		fullUpdate = true
	}

	if fullUpdate {
		return db.Insert(ctx, table, key, values)
	} else {
		keyValueMap, err := db.getFieldValueMap(ctx, key, table)
		if err != nil {
			return err
		}
		for k, v := range values {
			keyValueMap[k] = string(v)
		}
		return db.insertStringStringMap(ctx, key, table, keyValueMap)
	}
}

func (db *orcaDB) getFieldValueMap(ctx context.Context, key, table string) (map[string]string, error) {
	req := &rcppb.GetRequest{Key: key, Bucket: table}

	client := db.getContactServerGrpcClient()
	res, err := client.Get(ctx, req)

	if err != nil {
		return nil, fmt.Errorf("unexpected error on get: %v", err)
	}

	if res.Error == rcppb.ErrorType_NOT_FOUND {
		return nil, fmt.Errorf("key %s not found", key)
	}

	if !res.Success {
		// Retry with other server
		db.changeContactServer("")
		return db.getFieldValueMap(ctx, key, table)
	}

	var stringStringMap map[string]string
	if err := json.Unmarshal([]byte(res.Value), &stringStringMap); err != nil {
		return nil, fmt.Errorf("failed to decode inner JSON: %v", err)
	}

	return stringStringMap, nil
}

func (db *orcaDB) insertStringStringMap(ctx context.Context, key string, table string, valueStringMap map[string]string) error {
	valueBytes, err := json.Marshal(valueStringMap)
	if err != nil {
		return err
	}

	req := &rcppb.StoreRequest{Key: key, Bucket: table, Value: string(valueBytes)}

	client := db.getContactServerGrpcClient()
	res, err := client.Store(ctx, req)

	if err != nil {
		return fmt.Errorf("unexpected error on insert: %v", err)
	}

	if res.Success {
		return nil
	}

	// Redirected to a new leader
	if res.Error == rcppb.ErrorType_NOT_LEADER {
		db.changeContactServer(res.Value)
		return db.insertStringStringMap(ctx, key, table, valueStringMap)
	}

	// Retry with other server
	db.changeContactServer("")
	return db.insertStringStringMap(ctx, key, table, valueStringMap)
}
