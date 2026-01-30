package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"rcp/db"
	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CallbackReply struct {
	Value string
	Error error
}
type LogWithCallbackChannel struct {
	LogEntry        *orcapb.LogEntry
	CallbackChannel chan CallbackReply
}

type vote struct {
	nodeId  string
	term    int64
	granted bool
}

type ConfigNode struct {
	Id   string `json:"id"`
	IP   string `json:"ip"`
	Port string `json:"port"`
}

type ConfigFile struct {
	Nodes []ConfigNode `json:"nodes"`
}

type NodeConfig struct {
	NodeID             string
	Protocol           string
	Persistent         bool
	ConfigJSON         string
	ConfigFile         string
	K                  int
	BatchSize          int
	BackoffDec         int
	ConsensusTimeout   int
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatTimeout   int
}

func (cfg NodeConfig) Validate() error {
	if cfg.NodeID == "" {
		return errors.New("node id is required")
	}

	switch cfg.Protocol {
	case "rcp", "raft", "fraft":
	default:
		return fmt.Errorf("protocol can either be 'rcp', 'raft', or 'fraft'")
	}

	if cfg.ConfigJSON == "" && cfg.ConfigFile == "" {
		return errors.New("config json or config file must be provided")
	}

	return nil
}

type Node struct {
	orcapb.UnimplementedOrcaServer
	kvpb.UnimplementedKVStoreServer

	// Unchanged attributes, don't require mutex locking
	Id             string `json:"id"`
	Port           string `json:"port"`
	IP             string `json:"ip"`
	NodeAddressMap map[string]string
	ConnMap        map[string]*grpc.ClientConn
	ClientMap      map[string]orcapb.OrcaClient

	inFlightMessageCount map[string]int

	N                 int
	K                 int
	replicationQuorum int
	protocol          string

	BatchSize  int
	BackoffDec int64

	ConsensusTimeout   time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatTimeout   time.Duration

	// Not part of the protocol, safe to not use mutex for performance
	Live bool

	mutex sync.Mutex

	// Require mutex locking

	// Raft replication attributes
	isLeader      bool
	db            db.Database
	currentTerm   int64
	commitIndex   int64
	logBufferChan chan LogWithCallbackChannel // Read from HTTP request into this buffer
	nextIndex     map[string]int64
	matchIndex    map[string]int64

	// TODO: persist votedFor on disk
	votedFor string

	// Index upto where logs have been executed (inclusive)
	execIndex     int64
	DBCloseFunc   func() error
	isCandidate   bool
	electionTimer *time.Timer

	/// The below hash sets are used to prevent duplicate failure/recovery logs from being inserted.
	//
	// Example:
	// - When a leader detects that S2 has failed, it inserts `failed(S2)` into the log.
	// - However, this log entry is not executed (i.e., it does not decrease the count of alive nodes)
	//   until it has been replicated across the quorum.
	// - On the next heartbeat, the leader still sees S2 as down. Since the previous log entry hasn't been executed yet,
	//   S2 is still marked as alive in the system state.
	// - As a result, the leader might attempt to insert another `failed(S2)` log entry, leading to duplicates.
	//
	pendingFailureSet  map[string]struct{}
	pendingRecoverySet map[string]struct{}
	failedSet          map[string]struct{}

	indexToCallbackChannelMap map[int64]chan CallbackReply

	// Number of nodes with which initial connection has been established
	initialConnectionEstablished atomic.Int64

	//isReady is false until initial connection has been established with all other nodes
	isReady bool

	stepdownChan chan struct{}
	flushChans   map[string]chan struct{}
	pendingFlush map[string]bool
}

// constructor
func NewNode(cfg NodeConfig) (*Node, error) {
	var parsedConfig ConfigFile

	if cfg.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(cfg.ConfigJSON), &parsedConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config JSON from flag: %w", err)
		}
	} else {
		byteValue, err := os.ReadFile(cfg.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file (%s): %w", cfg.ConfigFile, err)
		}
		if err := json.Unmarshal(byteValue, &parsedConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config file (%s): %w", cfg.ConfigFile, err)
		}
	}

	if len(parsedConfig.Nodes) == 0 {
		return nil, errors.New("node configuration contains no nodes")
	}

	log.Printf("K: %d, batch size: %d, backoff decrement: %d, consensus timeout: %dms, election timeout: %dms-%dms, heartbeat timeout: %dms", cfg.K, cfg.BatchSize, cfg.BackoffDec, cfg.ConsensusTimeout, cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax, cfg.HeartbeatTimeout)
	for _, nodeDef := range parsedConfig.Nodes {
		log.Printf("%s %s %s", nodeDef.Id, nodeDef.IP, nodeDef.Port)
	}

	newNode := &Node{
		Id:                 cfg.NodeID,
		currentTerm:        0,
		K:                  cfg.K,
		BatchSize:          cfg.BatchSize,
		BackoffDec:         int64(cfg.BackoffDec),
		ConsensusTimeout:   time.Duration(cfg.ConsensusTimeout) * time.Millisecond,
		ElectionTimeoutMin: time.Duration(cfg.ElectionTimeoutMin) * time.Millisecond,
		ElectionTimeoutMax: time.Duration(cfg.ElectionTimeoutMax) * time.Millisecond,
		HeartbeatTimeout:   time.Duration(cfg.HeartbeatTimeout) * time.Millisecond,

		// lastApplied:           -1,
		commitIndex: -1,
		execIndex:   -1,
		// lastIndex:             -1,
		// lastTerm:              -1,
		nextIndex:            make(map[string]int64),
		matchIndex:           make(map[string]int64),
		NodeAddressMap:       make(map[string]string),
		ConnMap:              make(map[string]*grpc.ClientConn),
		Live:                 true,
		ClientMap:            make(map[string]orcapb.OrcaClient),
		electionTimer:        time.NewTimer(20 * time.Minute),
		logBufferChan:        make(chan LogWithCallbackChannel, 10000),
		inFlightMessageCount: make(map[string]int),
		pendingFailureSet:    make(map[string]struct{}),
		pendingRecoverySet:   make(map[string]struct{}),
		failedSet:            make(map[string]struct{}),
		// failureLogWaitingSet:  make(map[string]struct{}),
		// recoveryLogWaitingSet: make(map[string]struct{}),
		// reachableNodes:        make(map[string]struct{}),
		// beginTime: time.Now(),
		indexToCallbackChannelMap: make(map[int64]chan CallbackReply),
		stepdownChan:              make(chan struct{}),
		flushChans:                make(map[string]chan struct{}),
		pendingFlush:              make(map[string]bool),
	}

	if cfg.Persistent {
		if err := os.MkdirAll("./dbs", 0o755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
		dbPath := fmt.Sprintf("./dbs/%s.db", cfg.NodeID)
		boltDB, closeFunc, err := db.InitBoltDatabase(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to init boltdb: %w", err)
		}
		newNode.db = boltDB
		newNode.DBCloseFunc = closeFunc
	} else {
		newNode.db = db.InitMemoryDatabase()
	}

	switch cfg.Protocol {
	case "rcp":
		newNode.replicationQuorum = cfg.K + 1
		newNode.protocol = "rcp"
	case "raft":
		newNode.replicationQuorum = int(len(parsedConfig.Nodes)/2) + 1
		newNode.protocol = "raft"
	case "fraft":
		newNode.replicationQuorum = cfg.K + 1
		newNode.protocol = "fraft"
	default:
		return nil, fmt.Errorf("invalid protocol: %s", cfg.Protocol)
	}

	log.Printf("Replication Quorum size: %d", newNode.replicationQuorum)

	// go through all the nodes defined in config file and map them to their gRPC ports
	for _, nodeDef := range parsedConfig.Nodes {
		if nodeDef.Id == cfg.NodeID {
			newNode.Port = nodeDef.Port
		}
		newNode.NodeAddressMap[nodeDef.Id] = fmt.Sprintf("%s:%s", nodeDef.IP, nodeDef.Port)
		newNode.inFlightMessageCount[nodeDef.Id] = 0
		if nodeDef.Id != cfg.NodeID {
			newNode.flushChans[nodeDef.Id] = make(chan struct{}, 1)
			newNode.pendingFlush[nodeDef.Id] = false
		}
	}

	newNode.N = len(newNode.NodeAddressMap)
	if newNode.NodeAddressMap[cfg.NodeID] == "" {
		return nil, fmt.Errorf("no port specified for ID: %s in config JSON", cfg.NodeID)
	}

	newNode.resetElectionTimer()

	return newNode, nil
}

func (node *Node) Start() error {

	// establish gRPC connections with other nodes
	if err := node.establishConns(); err != nil {
		return err
	}

	for !node.isReady {
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("All cconnections established")

	go node.monitorElectionTimer()

	go node.startReceiverLoop(node.logBufferChan)

	return nil
}

// This function assume mutex is already locked
func (node *Node) GetLastTermLocked() int64 {
	lastTerm, err := node.db.GetLastTerm()
	if err != nil {
		log.Panicf("Error getting last term: %v", err)
	}

	return lastTerm
}

// This function assume mutex is already locked
func (node *Node) GetLastIndexLocked() int64 {
	lastIndex, err := node.db.GetLastIndex()
	if err != nil {
		log.Panicf("Error getting last index: %v", err)
	}

	return lastIndex
}

func (node *Node) HandleStore(key string, bucket string, value string) (string, error) {
	// log.Printf("Received data: Key=%s, Value=%s, Bucket=%s", key, value, bucket)

	callbackCh := make(chan CallbackReply, 1)
	// begin := time.Now()

	go func() {
		node.logBufferChan <- LogWithCallbackChannel{
			LogEntry: &orcapb.LogEntry{
				LogType: orcapb.LogType_OPERATION,
				Payload: marshalKVOperation(&kvpb.KVRequest{
					Op:     kvpb.OperationType_STORE,
					Key:    key,
					Value:  value,
					Bucket: bucket,
				}),
			},
			CallbackChannel: callbackCh,
		}
	}()

	select {
	case reply := <-callbackCh:
		// log.Printf("Time to get callback after put: %v, absolute: %v, and error: %v", time.Since(begin), time.Now().UnixMilli(), reply.Error)
		return reply.Value, reply.Error
	case <-time.After(node.ConsensusTimeout):
		// log.Printf("Time out to store key %s", key)
		return "", ErrTimeOut
	}
}

func (node *Node) HandleGet(key string, bucket string) (string, error) {
	// No lock used here for performance and cost of mistake is very low since
	// the get operation run first, if changes were supposed to happen, the data
	// would be the same as if lock is used
	value, err := node.db.Get(key, bucket)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (node *Node) HandleDelete(key string, bucket string) (string, error) {
	log.Printf("Received delete: Key=%s, Bucket=%s", key, bucket)

	callbackCh := make(chan CallbackReply, 1)
	begin := time.Now()

	go func() {
		node.logBufferChan <- LogWithCallbackChannel{
			LogEntry: &orcapb.LogEntry{
				LogType: orcapb.LogType_OPERATION,
				Payload: marshalKVOperation(&kvpb.KVRequest{
					Op:     kvpb.OperationType_DELETE,
					Key:    key,
					Bucket: bucket,
				}),
			},
			CallbackChannel: callbackCh,
		}
	}()

	select {
	case reply := <-callbackCh:
		log.Printf("Time to get callback after delete: %v", time.Since(begin))
		return reply.Value, reply.Error
	case <-time.After(node.ConsensusTimeout):
		log.Printf("TIMED OUT Delete key: %s, bucket: %s", key, bucket)
		return "", ErrTimeOut
	}
}

func marshalKVOperation(req *kvpb.KVRequest) []byte {
	payload, err := proto.Marshal(req)
	if err != nil {
		log.Panicf("failed to marshal kv operation: %v", err)
	}
	return payload
}

// This function assume mutex is already locked
func (node *Node) StepDownLocked() {
	// Shutdown all replication loop
	close(node.stepdownChan)

	node.isLeader = false
	node.isCandidate = false
	node.stepdownChan = make(chan struct{})

	// log.Println("Reset election timer")
	node.resetElectionTimer()
}

// This function assume mutex is already locked
func (node *Node) BecomeLeaderLocked() {
	log.Println("Became leader!")

	node.isLeader = true
	node.isCandidate = false

	// Stop election timer
	if !node.electionTimer.Stop() {
		select {
		case <-node.electionTimer.C:
		default:
		}
	}

	// Start replication loop
	for nodeId := range node.ClientMap {
		node.nextIndex[nodeId] = node.GetLastIndexLocked() + 1
		node.matchIndex[nodeId] = -1
		go node.startHeartbeatLoop(nodeId)
	}
}

// This function assume mutex is already locked
func (node *Node) AppendLogLocked(logEntry *orcapb.LogEntry) int64 {
	if logEntry.LogType == orcapb.LogType_FAILURE {
		node.pendingFailureSet[logEntry.NodeId] = struct{}{}
	}

	if logEntry.LogType == orcapb.LogType_RECOVERY {
		delete(node.failedSet, logEntry.NodeId)
		node.pendingRecoverySet[logEntry.NodeId] = struct{}{}
	}

	currIdx, err := node.db.AppendLog(logEntry)
	if err != nil {
		log.Panicf("Error appending log: %v", err)
	}

	return currIdx
}

// This function assume mutex is already locked
func (node *Node) InsertLogLocked(logEntry *orcapb.LogEntry, idx int64) {
	existingEntry, err := node.db.GetLogAtIndex(idx)

	if err == nil {
		if existingEntry.LogType == orcapb.LogType_FAILURE {
			delete(node.pendingFailureSet, existingEntry.NodeId)
		}

		if existingEntry.LogType == orcapb.LogType_RECOVERY {
			delete(node.pendingRecoverySet, existingEntry.NodeId)
			node.failedSet[logEntry.NodeId] = struct{}{}
		}
	}

	if logEntry.LogType == orcapb.LogType_FAILURE {
		node.pendingFailureSet[logEntry.NodeId] = struct{}{}
	}

	if logEntry.LogType == orcapb.LogType_RECOVERY {
		node.pendingRecoverySet[logEntry.NodeId] = struct{}{}
	}

	err = node.db.PutLogAtIndex(idx, logEntry)
	if err != nil {
		log.Panicf("Error putting log at index %d: %v", idx, err)
	}
}

// request votes from other nodes on election timer expiry
func (node *Node) requestVotes() {
	log.Println("Requesting votes, if stuck here, deadlock occured")
	node.mutex.Lock()
	log.Println("Requesting votes start")

	electionQuorum := node.N - node.K
	if node.protocol == "raft" {
		electionQuorum = (node.N / 2) + 1
	} else if node.protocol == "rcp" {
		electionQuorum = electionQuorum - len(node.failedSet) - len(node.pendingRecoverySet)
	}

	node.currentTerm++
	log.Printf("Starting election with quorum size %d and term %d", electionQuorum, node.currentTerm)

	node.isCandidate = true
	node.votedFor = node.Id
	// log.Println("Reset election timer")
	node.resetElectionTimer()

	node.mutex.Unlock()

	// initialized to 1 because already voted for self
	voteCount := 1
	// create channel to collect votes
	votesCh := make(chan vote, len(node.ClientMap))

	ctx, cancel := context.WithCancel(context.Background())

	for nodeId, client := range node.ClientMap {
		go node.sendRequestVote(client, ctx, node.currentTerm, votesCh, nodeId)
	}

	timeout := time.After(node.ElectionTimeoutMax)

	for voteCount < electionQuorum {
		select {
		case vote := <-votesCh:
			node.mutex.Lock()

			if vote.term > node.currentTerm {
				node.currentTerm = vote.term
				node.StepDownLocked()
				node.mutex.Unlock()
				cancel()
				return
			}

			if vote.granted {
				// Ignore if it's from failed node
				if _, failed := node.failedSet[vote.nodeId]; failed {
					node.mutex.Unlock()
					continue
				}

				// Ignore if it's from pending recovery node
				if _, pendingRecovery := node.pendingRecoverySet[vote.nodeId]; pendingRecovery {
					node.mutex.Unlock()
					continue
				}

				voteCount++
			}

			log.Printf("Current vote count: %d", voteCount)
			node.mutex.Unlock()
		case <-timeout:
			log.Println("Election timeout")
			cancel()
			return
		}
	}

	node.mutex.Lock()
	if node.isCandidate {
		node.BecomeLeaderLocked()
	}
	node.mutex.Unlock()
	cancel()
}

func (node *Node) sendRequestVote(client orcapb.OrcaClient, ctx context.Context, term int64, votesChan chan vote, nodeId string) {
	log.Printf("Sending RequestVote to %s\n", nodeId)

	resp, err := client.RequestVote(context.Background(), &orcapb.RequestVoteRequest{
		Term:         term,
		CandidateId:  node.Id,
		LastLogIndex: node.GetLastIndexLocked(),
		LastLogTerm:  node.GetLastTermLocked(),
	})

	log.Printf("Resp from %s: %v", nodeId, resp)
	var voteReply vote

	if err != nil {
		voteReply = vote{
			nodeId:  nodeId,
			term:    -1,
			granted: false,
		}
	} else {
		voteReply = vote{
			nodeId:  nodeId,
			term:    resp.Term,
			granted: resp.VoteGranted,
		}
	}

	select {
	case <-ctx.Done():
		return
	case votesChan <- voteReply:
	}
}
