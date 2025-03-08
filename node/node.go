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
	currentTerm     int64
	votedFor        sync.Map
	db              *db.Database
	commitIndex     int64
	execIndex       int64
	lastApplied     int64
	nextIndex       sync.Map
	matchIndex      map[string]int
	lastIndex       int64
	lastTerm        int64
	Id              string            `json:"id"`
	Port            string            `json:"port"`
	HttpPort        string            `json:"http_port"`
	NodeMap         map[string]string `json:"nodeMap"`
	ConnMap         map[string]*grpc.ClientConn
	ClientMap       map[string]rcppb.RCPClient
	Live            bool
	DBCloseFunc     func() error
	K               int
	isLeader        bool
	isCandidate     bool
	electionTimer   *time.Timer
	currAlive       int64
	serverStatusMap sync.Map
	logBufferChan   chan *rcppb.LogEntry // Read from HTTP request into this buffer

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
}

type ConfigFile struct {
	K     int     `json:"K"`
	Nodes []*Node `json:"nodes"`
}

func NewNode(nodeId string) (*Node, error) {

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

	dbPath := "./dbs/" + nodeId + ".db"
	db, dbCloseFunc, err := db.InitDatabase(dbPath)
	if err != nil {
		log.Fatalf("InitDatabase(%q): %v", dbPath, err)
	}

	matchIndexMap := make(map[string]int)
	nodes = config.Nodes

	newNode := &Node{
		Id:                    nodeId,
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
		logBufferChan:         make(chan *rcppb.LogEntry),
		failureLogWaitingSet:  make(map[string]struct{}),
		recoveryLogWaitingSet: make(map[string]struct{}),
	}

	for _, node := range nodes {
		if node.Id == nodeId {
			newNode.HttpPort = node.HttpPort
			newNode.Port = node.Port
		}
		newNode.NodeMap[node.Id] = node.Port
		newNode.serverStatusMap.Store(node.Id, true)
	}
	newNode.currAlive = int64(len(newNode.NodeMap))
	if newNode.NodeMap[nodeId] == "" {
		log.Fatalf("No port specified for ID: %s in config JSON", nodeId)
	}

	newNode.resetElectionTimer()

	return newNode, nil
}

func (node *Node) Start() {
	go node.startHttpServer()
	time.Sleep(5 * time.Second)
	node.establishConns()
	node.initNextIndex()
	go node.monitorElectionTimer()
	go node.sendHeartbeats()
	go node.executor()
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

func (node *Node) requestVotes() {
	log.Printf("Requesting votes\n")
	node.currentTerm += 1
	node.votedFor.Store(node.currentTerm, node.Id)
	node.isCandidate = true
	term := node.currentTerm
	votesChan := make(chan *rcppb.RequestVoteResponse, len(node.ClientMap))
	for nodeId, client := range node.ClientMap {
		go node.sendRequestVote(client, term, votesChan, nodeId)
	}
	voteCount := int64(1)
	voteWaitTimer := time.After(500 * time.Millisecond)
	for {
		select {
		case voteResp := <-votesChan:
			log.Printf("Received vote %t\n", voteResp.VoteGranted)
			if voteResp.Term > term {
				node.currentTerm = voteResp.Term
				node.isCandidate = false
				return
			}
			if voteResp.VoteGranted {
				voteCount += 1
				if voteCount >= node.currAlive-int64(node.K) {
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
	resp, _ := client.RequestVote(context.Background(), &rcppb.RequestVoteReq{
		Term:         term,
		CandidateId:  node.Id,
		LastLogIndex: node.lastIndex,
		LastLogTerm:  node.lastTerm,
	})

	votesChan <- resp
}
