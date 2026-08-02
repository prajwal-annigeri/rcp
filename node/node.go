package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"rcp/db"
	"rcp/rcppb"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

var nodes []*Node
var config ConfigFile

const (
	ReconfigModeNone    = "none"
	ReconfigModeJoint   = "joint"
	ReconfigModeRecraft = "recraft"
	ReconfigModeOrca    = "orca"
)

type CallbackReply struct {
	Value string
	Error error
}
type LogWithCallbackChannel struct {
	LogEntry        *rcppb.LogEntry
	CallbackChannel chan CallbackReply
}

type vote struct {
	nodeId  string
	term    int64
	granted bool
}
type Node struct {
	rcppb.UnimplementedRCPServer

	// Unchanged attributes, don't require mutex locking
	Id             string `json:"id"`
	Port           string `json:"port"`
	HttpPort       string `json:"http_port"`
	IP             string `json:"ip"`
	NodeAddressMap map[string]string
	ConnMap        map[string]*grpc.ClientConn
	ClientMap      map[string]rcppb.RCPClient

	inFlightMessageCount map[string]int

	N                 int
	K                 int
	replicationQuorum int
	protocol          string
	reconfigMode      string

	BatchSizeLow  int
	BatchSizeHigh int
	BackoffDec    int64

	ConsensusTimeout   time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	BatchTimeout       time.Duration
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
	execIndex int64
	// lastApplied int64
	// lastIndex       int64
	// lastTerm        int64
	DBCloseFunc   func() error
	isCandidate   bool
	electionTimer *time.Timer
	// currAlive       int
	// serverStatusMap sync.Map
	// beginTime                      time.Time
	// possibleFailureOrRecoveryIndex sync.Map

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

	// failureLogWaitingSet  map[string]struct{}
	// recoveryLogWaitingSet map[string]struct{}
	// failureSetLock        sync.Mutex
	// recoverySetLock       sync.Mutex

	// reachable nodes set to simulate partitions
	// reachableNodes   map[string]struct{}
	// reachableSetLock sync.RWMutex

	indexToCallbackChannelMap map[int64]chan CallbackReply

	// failedAppendEntries sync.Map
	// replicatedCount     sync.Map
	// delays sync.Map

	// Number of nodes with which initial connection has been established
	initialConnectionEstablished atomic.Int64

	//isReady is false until initial connection has been established with all other nodes
	isReady bool

	stepdownChan chan struct{}

	// Reconfiguration scaffolding state
	knownNodeSet                map[string]struct{}
	activeVoterSet              map[string]struct{}
	pendingVoterSet             map[string]struct{}
	transitionVoterSet          map[string]struct{}
	transitionElectionQuorum    int
	transitionReplicationQuorum int
	transitionStep              int
	reconfigInFlight            bool
	reconfigEpoch               int64
	reconfigCurrentPhase        string
	lastLeaderContact           time.Time
}

// struct to read in the config file
type ConfigFile struct {
	Nodes []*Node `json:"nodes"`
}

// constructor
func NewNode(
	thisNodeId,
	protocol string,
	reconfigMode string,
	persistent bool,
	configString,
	configFile string,
	K int,
	batchSizeLow int,
	batchSizeHigh int,
	backoffDec int,
	consensusTimeout int,
	electionTimeoutMin int,
	electionTimeoutMax int,
	batchTimeout int,
	heartbeatTimeout int) (*Node, error) {
	rawReconfigMode := reconfigMode
	reconfigMode = normalizeReconfigMode(reconfigMode)
	if reconfigMode == "" {
		return nil, fmt.Errorf("invalid reconfig mode %q: supported values are %s, %s, %s, %s", rawReconfigMode, ReconfigModeNone, ReconfigModeJoint, ReconfigModeRecraft, ReconfigModeOrca)
	}

	if protocol != "raft" && reconfigMode != ReconfigModeNone {
		return nil, fmt.Errorf("reconfiguration is supported only when protocol=raft; got protocol=%s reconfig-mode=%s", protocol, reconfigMode)
	}

	if configString == "" {
		// reads config file
		nodesJson, err := os.Open(configFile)
		if err != nil {
			log.Fatalf("Error with reading config JSON (%s) Provide config as command line argument --config or --config-file or put config in nodes.json: %s\n", configFile, err)
		}
		defer nodesJson.Close()

		byteValue, err := io.ReadAll(nodesJson)
		if err != nil {
			log.Fatalf("Failed to read file: %s", err)
		}
		err = json.Unmarshal(byteValue, &config)
		if err != nil {
			log.Fatalf("Failed to unmarshal JSON: %s", err)
		}
	} else {
		err := json.Unmarshal([]byte(configString), &config)
		if err != nil {
			log.Fatalf("Failed to unmarshal JSON from command line arg: %s", err)
		}
	}

	log.Printf("K: %d, batch size: %d-%d, backoff decrement: %d, consensus timeout: %dms, election timeout: %dms-%dms, batch timeout: %dms, heartbeat timeout: %dms", K, batchSizeLow, batchSizeHigh, backoffDec, consensusTimeout, electionTimeoutMin, electionTimeoutMax, batchTimeout, heartbeatTimeout)
	for _, node := range config.Nodes {
		log.Printf("%s %s %s %s", node.Id, node.IP, node.Port, node.HttpPort)
	}

	nodes = config.Nodes

	newNode := &Node{
		Id:                 thisNodeId,
		currentTerm:        0,
		K:                  K,
		BatchSizeLow:       batchSizeLow,
		BatchSizeHigh:      batchSizeHigh,
		BackoffDec:         int64(backoffDec),
		ConsensusTimeout:   time.Duration(consensusTimeout) * time.Millisecond,
		ElectionTimeoutMin: time.Duration(electionTimeoutMin) * time.Millisecond,
		ElectionTimeoutMax: time.Duration(electionTimeoutMax) * time.Millisecond,
		BatchTimeout:       time.Duration(batchTimeout) * time.Millisecond,
		HeartbeatTimeout:   time.Duration(heartbeatTimeout) * time.Millisecond,

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
		ClientMap:            make(map[string]rcppb.RCPClient),
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
	}

	// if persistent {
	// 	// Initialize data store
	// 	dbPath := "./dbs/" + thisNodeId + ".db"
	// 	db, dbCloseFunc, err := db.InitBoltDatabase(dbPath)
	// 	if err != nil {
	// 		log.Fatalf("InitDatabase(%q): %v", dbPath, err)
	// 	}
	// 	newNode.db = db
	// 	newNode.DBCloseFunc = dbCloseFunc
	// } else {
	// 	newNode.db = db.InitMemoryDatabase()
	// }
	newNode.db = db.InitMemoryDatabase()

	switch protocol {
	case "rcp":
		newNode.replicationQuorum = K + 1
		newNode.protocol = "rcp"
	case "raft":
		newNode.replicationQuorum = int(len(nodes)/2) + 1
		newNode.protocol = "raft"
	case "fraft":
		newNode.replicationQuorum = K + 1
		newNode.protocol = "fraft"
	default:
		log.Fatalf("Invalid protocol: %s", protocol)
	}

	log.Printf("Replication Quorum size: %d", newNode.replicationQuorum)
	newNode.reconfigMode = reconfigMode

	// go through all the nodes defined in config file and map them to their gRPC ports
	for _, node := range nodes {
		// if current node, then assign ports to node object
		if node.Id == thisNodeId {
			newNode.HttpPort = node.HttpPort
			newNode.Port = node.Port
		}
		newNode.NodeAddressMap[node.Id] = fmt.Sprintf("%s:%s", normalizeDialHost(node.IP), node.Port)
		newNode.inFlightMessageCount[node.Id] = 0
		// newNode.serverStatusMap.Store(node.Id, true)
		// newNode.reachableNodes[node.Id] = struct{}{}
		// newNode.failedAppendEntries.Store(thisNodeId, 0)
		// newNode.delays.Store(node.Id, int64(0))
	}

	newNode.knownNodeSet = make(map[string]struct{}, len(nodes))
	newNode.activeVoterSet = make(map[string]struct{}, len(nodes))
	newNode.pendingVoterSet = make(map[string]struct{})
	newNode.transitionVoterSet = make(map[string]struct{})
	newNode.transitionElectionQuorum = 0
	newNode.transitionReplicationQuorum = 0
	newNode.transitionStep = 0
	for _, cfgNode := range nodes {
		newNode.knownNodeSet[cfgNode.Id] = struct{}{}
		newNode.activeVoterSet[cfgNode.Id] = struct{}{}
	}
	newNode.reconfigCurrentPhase = "stable"
	newNode.lastLeaderContact = time.Now()

	// initialize current alive to number of nodes in the config file
	newNode.N = len(newNode.NodeAddressMap)
	if newNode.NodeAddressMap[thisNodeId] == "" {
		log.Fatalf("No port specified for ID: %s in config JSON", thisNodeId)
	}

	// log.Println("Reset election timer")
	newNode.resetElectionTimer()

	return newNode, nil
}

func (node *Node) Start() {

	// starts HTTP server used by clients to interact with server
	// go node.startHttpServer()
	// time.Sleep(1 * time.Second)

	// initialize next index (log of index to send to a node) for every node to 0
	// node.initNextIndex()

	// establish gRPC connections with ohter nodes
	node.establishConns()

	for !node.isReady {
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("ALL CONNECTIONS ESTABLISHED")
	// start goroutine that monitors the election timer
	go node.monitorElectionTimer()

	// start goroutine that sends heartbeats/AppendEntries
	// go node.sendHeartbeats()
	go node.startReceiverLoop(node.logBufferChan)

	//start executor goroutine which applies logs to state machine
	// go node.executor()

	// go node.callbacker()

	// Keep the node process alive in headless mode.
	select {}
}

func normalizeReconfigMode(reconfigMode string) string {
	switch reconfigMode {
	case ReconfigModeNone, ReconfigModeJoint, ReconfigModeRecraft, ReconfigModeOrca:
		return reconfigMode
	default:
		return ""
	}
}

func normalizeDialHost(host string) string {
	if host == "localhost" {
		return "127.0.0.1"
	}
	return host
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
			LogEntry: &rcppb.LogEntry{
				LogType: rcppb.LogType_STORE,
				Key:     key,
				Value:   value,
				Bucket:  bucket,
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
			LogEntry: &rcppb.LogEntry{
				LogType: rcppb.LogType_DELETE,
				Key:     key,
				Bucket:  bucket,
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

// This function assume mutex is already locked
func (node *Node) StepDownLocked() {
	// Shutdown all replication loop
	if node.stepdownChan != nil {
		close(node.stepdownChan)
	}

	node.isLeader = false
	node.isCandidate = false

	// "in-flight while stable" is a leader-local speculative request flag
	// (set when a leader appends reconfig entries but before they execute).
	// If this leader steps down before commit, clear it so a future leader can
	// accept new reconfiguration requests.
	if node.reconfigCurrentPhase == reconfigPhaseStable {
		node.reconfigInFlight = false
	}

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
func (node *Node) AppendLogLocked(logEntry *rcppb.LogEntry) int64 {
	if logEntry.LogType == rcppb.LogType_FAILURE {
		node.pendingFailureSet[logEntry.NodeId] = struct{}{}
	}

	if logEntry.LogType == rcppb.LogType_RECOVERY {
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
func (node *Node) InsertLogLocked(logEntry *rcppb.LogEntry, idx int64) {
	existingEntry, err := node.db.GetLogAtIndex(idx)

	if err == nil {
		if existingEntry.LogType == rcppb.LogType_FAILURE {
			delete(node.pendingFailureSet, existingEntry.NodeId)
		}

		if existingEntry.LogType == rcppb.LogType_RECOVERY {
			delete(node.pendingRecoverySet, existingEntry.NodeId)
			node.failedSet[logEntry.NodeId] = struct{}{}
		}
	}

	if logEntry.LogType == rcppb.LogType_FAILURE {
		node.pendingFailureSet[logEntry.NodeId] = struct{}{}
	}

	if logEntry.LogType == rcppb.LogType_RECOVERY {
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

	if node.protocol == "raft" {
		if !node.isElectionVoterLocked(node.Id) {
			log.Printf("Skipping election because node %s is not an election voter in current configuration phase", node.Id)
			node.isCandidate = false
			node.mutex.Unlock()
			return
		}

		node.currentTerm++
		log.Printf("Starting election in term %d with reconfig mode %s and phase %s", node.currentTerm, node.reconfigMode, node.reconfigCurrentPhase)

		node.isCandidate = true
		node.votedFor = node.Id
		node.resetElectionTimer()

		selfVoteSet := map[string]struct{}{
			node.Id: {},
		}
		if node.hasElectionQuorumLocked(selfVoteSet) {
			node.BecomeLeaderLocked()
			node.mutex.Unlock()
			return
		}

		term := node.currentTerm
		node.mutex.Unlock()

		votes := map[string]struct{}{
			node.Id: {},
		}

		votesCh := make(chan vote, len(node.ClientMap))
		ctx, cancel := context.WithCancel(context.Background())
		for nodeID, client := range node.ClientMap {
			go node.sendRequestVote(client, ctx, term, votesCh, nodeID)
		}

		timeout := time.After(node.ElectionTimeoutMax)

		for {
			select {
			case vote := <-votesCh:
				node.mutex.Lock()

				if vote.term > node.currentTerm {
					node.currentTerm = vote.term
					node.votedFor = ""
					node.StepDownLocked()
					node.mutex.Unlock()
					cancel()
					return
				}

				if vote.granted {
					if node.isElectionVoterLocked(vote.nodeId) {
						votes[vote.nodeId] = struct{}{}
					}
				}

				if node.hasElectionQuorumLocked(votes) {
					if node.isCandidate {
						node.BecomeLeaderLocked()
					}
					node.mutex.Unlock()
					cancel()
					return
				}

				node.mutex.Unlock()
			case <-timeout:
				log.Println("Election timeout")
				cancel()
				return
			}
		}
	}

	electionQuorum := node.N - node.K
	if node.protocol == "raft" {
		electionQuorum = (len(nodes) / 2) + 1
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
				node.votedFor = ""
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

// func (node *Node) initNextIndex() {
// 	for _, otherNode := range nodes {
// 		node.nextIndex.Store(otherNode.Id, node.lastIndex+1)
// 	}
// }

func (node *Node) sendRequestVote(client rcppb.RCPClient, ctx context.Context, term int64, votesChan chan vote, nodeId string) {
	log.Printf("Sending RequestVote to %s\n", nodeId)

	// delayRaw, ok := node.delays.Load(nodeId)
	// var delay int64
	// if !ok {
	// 	delay = 0
	// } else {
	// 	delay = delayRaw.(int64)
	// }
	resp, err := client.RequestVote(context.Background(), &rcppb.RequestVoteReq{
		Term:         term,
		CandidateId:  node.Id,
		LastLogIndex: node.GetLastIndexLocked(),
		LastLogTerm:  node.GetLastTermLocked(),
		// Delay:        int64(delay),
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
