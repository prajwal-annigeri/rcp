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
	"time"

	"google.golang.org/grpc"
)

var nodes []*Node
var config ConfigFile

type Node struct {
	rcppb.UnimplementedRCPServer
	currentTerm int64

	// TODO: persist votedFor on disk
	votedFor          sync.Map
	db                *db.Database
	commitIndex       int64
	execIndex         int64
	lastApplied       int64
	nextIndex         sync.Map
	matchIndex        map[string]int
	lastIndex         int64
	lastTerm          int64
	Id                string            `json:"id"`
	Port              string            `json:"port"`
	HttpPort          string            `json:"http_port"`
	NodeMap           map[string]string `json:"nodeMap"`
	ConnMap           map[string]*grpc.ClientConn
	ClientMap         map[string]rcppb.RCPClient
	Live              bool
	DBCloseFunc       func() error
	K                 int
	isLeader          bool
	isCandidate       bool
	electionTimer     *time.Timer
	currAlive         int
	serverStatusMap   sync.Map
	logBufferChan     chan *rcppb.LogEntry // Read from HTTP request into this buffer
	mutex             sync.Mutex
	replicationQuorum int
	protocol          string

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
	failureLogWaitingSet  map[string]struct{}
	recoveryLogWaitingSet map[string]struct{}
	failureSetLock        sync.Mutex
	recoverySetLock       sync.Mutex

	// reachable nodes set to simulate partitions
	reachableNodes     map[string]struct{}
	reachableSetLock   sync.RWMutex
	callbackChannelMap sync.Map

	failedAppendEntries sync.Map
	// replicatedCount     sync.Map
	delays sync.Map
}

// struct to read in the config file
type ConfigFile struct {
	K     int     `json:"K"`
	Nodes []*Node `json:"nodes"`
}

// constructor
func NewNode(thisNodeId, protocol string) (*Node, error) {
	// reads config file
	mapJson, err := os.Open("nodes.json")
	if err != nil {
		log.Fatalf("Error with reading config JSON: %s\n", err)
	}
	defer mapJson.Close()

	byteValue, err := io.ReadAll(mapJson)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %s", err)
	}

	// Initialize data store
	dbPath := "./dbs/" + thisNodeId + ".db"
	db, dbCloseFunc, err := db.InitDatabase(dbPath)
	if err != nil {
		log.Fatalf("InitDatabase(%q): %v", dbPath, err)
	}

	matchIndexMap := make(map[string]int)
	nodes = config.Nodes

	newNode := &Node{
		Id:                    thisNodeId,
		currentTerm:           0,
		K:                     config.K,
		db:                    db,
		DBCloseFunc:           dbCloseFunc,
		lastApplied:           -1,
		commitIndex:           -1,
		execIndex:             -1,
		lastIndex:             -1,
		lastTerm:              -1,
		matchIndex:            matchIndexMap,
		NodeMap:               make(map[string]string),
		ConnMap:               make(map[string]*grpc.ClientConn),
		Live:                  true,
		ClientMap:             make(map[string]rcppb.RCPClient),
		electionTimer:         time.NewTimer(2 * time.Second),
		logBufferChan:         make(chan *rcppb.LogEntry, 10000),
		failureLogWaitingSet:  make(map[string]struct{}),
		recoveryLogWaitingSet: make(map[string]struct{}),
		reachableNodes:        make(map[string]struct{}),
	}

	if protocol == "rcp" {
		newNode.replicationQuorum = config.K + 1
		newNode.protocol = "rcp"
	} else if protocol == "raft" {
		newNode.replicationQuorum = int(len(nodes)/2) + 1
		newNode.protocol = "raft"
	} else if protocol == "fraft" {
		newNode.replicationQuorum = config.K + 1
		newNode.protocol = "fraft"
	} else {
		log.Fatalf("Invalid protocol: %s", protocol)
	}

	log.Printf("Replication Quorum size: %d", newNode.replicationQuorum)

	// go through all the nodes defined in config file and map them to their gRPC ports
	for _, node := range nodes {
		// if current node, then assign ports to node object
		if node.Id == thisNodeId {
			newNode.HttpPort = node.HttpPort
			newNode.Port = node.Port
		}
		newNode.NodeMap[node.Id] = node.Port
		newNode.serverStatusMap.Store(node.Id, true)
		newNode.reachableNodes[node.Id] = struct{}{}
		newNode.failedAppendEntries.Store(thisNodeId, 0)
		newNode.delays.Store(node.Id, int64(0))
	}

	// initialize current alive to number of nodes in the config file
	newNode.currAlive = len(newNode.NodeMap)
	if newNode.NodeMap[thisNodeId] == "" {
		log.Fatalf("No port specified for ID: %s in config JSON", thisNodeId)
	}

	newNode.resetElectionTimer()

	return newNode, nil
}

func (node *Node) Start() {

	// starts HTTP server used by clients to interact with server
	// go node.startHttpServer()
	time.Sleep(1 * time.Second)

	// establish gRPC connections with ohter nodes
	node.establishConns()

	// initialize next index (log of index to send to a node) for every node to 0
	node.initNextIndex()

	// start goroutine that monitors the election timer
	go node.monitorElectionTimer()

	// start goroutine that sends heartbeats/AppendEntries
	go node.sendHeartbeats()

	//start executor goroutine which applies logs to state machine
	go node.executor()

	go node.callbacker()

	for {
		printMenu()
		var input string
		fmt.Scan(&input)

		switch input {
		case "2":
			node.db.PrintAllLogs()
		case "3":
			err := node.db.PrintAllLogsUnordered()
			if err != nil {
				log.Printf("Error printing all logs: %v\n", err)
			}
		case "4":
			node.printState()
		default:
			fmt.Println("Invalid option. Please choose again.")
		}
	}
}

// request votes from other nodes on election timer expiry
func (node *Node) requestVotes() {
	log.Printf("Requesting votes\n")
	electionQuorum := node.currAlive - node.K
	if node.protocol == "raft" {
		electionQuorum = (len(nodes) / 2) + 1
	}
	node.currentTerm += 1

	// vote for self
	// TODO: call RequestVote on self rather than doing this?
	node.votedFor.Store(node.currentTerm, node.Id)
	node.isCandidate = true
	term := node.currentTerm

	// create channel to collect votes
	votesChan := make(chan *rcppb.RequestVoteResponse, len(node.ClientMap))

	// send RequestVote to every other node
	node.reachableSetLock.RLock()
	for nodeId := range node.reachableNodes {
		client, ok := node.ClientMap[nodeId]
		if !ok {
			continue
		}
		go node.sendRequestVote(client, term, votesChan, nodeId)
	}
	node.reachableSetLock.RUnlock()

	voteCount := 1 // initialized to 1 because already voted for self

	// initialize timer to wait for votes
	// TODO: refine timer duration
	voteWaitTimer := time.After(500 * time.Millisecond)
	for {
		select {
		// read in vote from channel
		case voteResp := <-votesChan:
			log.Printf("Received vote %v\n", voteResp)
			// validate vote
			if voteResp != nil && voteResp.Term > term {
				node.currentTerm = voteResp.Term
				node.isCandidate = false
				return
			}
			if voteResp != nil && voteResp.VoteGranted {
				voteCount += 1
				if voteCount >= electionQuorum {
					node.isLeader = true
					node.isCandidate = false
					node.electionTimer.Stop()
					log.Println("Became leader!")
					go node.initNextIndex()
					return
				}
			}
		case <-voteWaitTimer:
			node.isCandidate = false
			return
		}
	}

}

func (node *Node) initNextIndex() {
	for _, otherNode := range nodes {
		node.nextIndex.Store(otherNode.Id, node.lastIndex+1)
	}
}

func (node *Node) sendRequestVote(client rcppb.RCPClient, term int64, votesChan chan *rcppb.RequestVoteResponse, nodeId string) {
	log.Printf("Sending RequestVote to %s\n", nodeId)
	delayRaw, ok := node.delays.Load(nodeId)
	var delay int64
	if !ok {
		delay = 0
	} else {
		delay = delayRaw.(int64)
	}
	resp, _ := client.RequestVote(context.Background(), &rcppb.RequestVoteReq{
		Term:         term,
		CandidateId:  node.Id,
		LastLogIndex: node.lastIndex,
		LastLogTerm:  node.lastTerm,
		Delay:        int64(delay),
	})
	log.Printf("Resp from %s: %v", nodeId, resp)
	votesChan <- resp
}
