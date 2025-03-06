package node

import (
	"context"
	"log"
	// "math/rand"
	"rcp/rcppb"
	"strconv"
	"time"
)

var printTimer bool

func randomElectionTimeout(nodeNum int) time.Duration {
	x := time.Duration(250 + nodeNum * 300) * time.Millisecond
	// x := time.Duration(250 + rand.Intn(2500)) * time.Millisecond
	if !printTimer {
		log.Println(x)
		printTimer = true
	}
	// log.Println(x)
	return x
}

func (node *Node) resetElectionTimer() {
	nodeNum, err := strconv.Atoi(node.Id[1:])
	if err != nil {
		log.Printf("Error setting timer: %v\n", err)
		return
	}
	
	node.electionTimer.Reset(randomElectionTimeout(nodeNum))
}


func (node *Node) monitorElectionTimer() {
	for {
        <-node.electionTimer.C
		if node.Live && !node.isLeader {
			log.Printf("Election timer ran out\n")
			go node.requestVotes()
		}
		node.resetElectionTimer()
		// node.electionTimer.Reset(node.randomElectionTimeout())
    }
}

func (node *Node) sendHeartbeats() {
	
	for {
		if node.Live && node.isLeader {
			doneReading := false
			var logsToSend []*rcppb.LogEntry
			for {
				select {
				case c := <- node.logBufferChan:
					log.Printf("Read log from channel: %v\n", c)
					c.Term = node.currentTerm
					logsToSend = append(logsToSend, c)
				default:
					doneReading = true
				}
				if doneReading {
					break
				}
			}

			// Call AppendEntries on leader
			node.AppendEntries(context.Background(), &rcppb.AppendEntriesReq{
				Term: node.currentTerm,
				LeaderId: node.Id,
				PrevLogIndex: node.lastIndex,
				LeaderCommit: node.commitIndex,
				PrevLogTerm: node.lastTerm,
				Entries: logsToSend,
			})

			// Construct AppendEntries Request
			// appendEntriesReq := node.constructAppendEntriesRequest(node.currentTerm, logsToSend)

			

			responseChan := make(chan *rcppb.AppendEntriesResponse)

			// Send AppendEntries to all other nodes
			for nodeId, client := range node.ClientMap {
				go node.sendHeartbeatTo(client, nodeId, responseChan)
			}

			successResponses, maxTerm := countSuccessfulAppendEntries(responseChan, 100 * time.Millisecond)
			if maxTerm > node.currentTerm {
				node.currentTerm = maxTerm
				node.isLeader = false
			} else if successResponses >= node.K + 1 {
				node.commitIndex = node.lastIndex //  + int64(len(appendEntriesReq.Entries))
			}

		} else{
			time.Sleep(HeartbeatInterval)
		}
		
	}
}

func (node *Node) constructAppendEntriesRequest(term int64, nodeId string) (*rcppb.AppendEntriesReq, error) {
	nextIndex, _ := node.nextIndex.Load(nodeId)
	entries, err := node.db.GetLogsFromIndex(nextIndex.(int64))
	if err != nil {
		return nil, err
	}
	nextLogIndex, _ := node.nextIndex.Load(nodeId)
	return &rcppb.AppendEntriesReq{
		Term: term,
		LeaderId: node.Id,
		PrevLogIndex: int64(nextLogIndex.(int64) - 1),
		LeaderCommit: node.commitIndex,
		PrevLogTerm: node.lastTerm,
		Entries: entries,
	}, nil
}

func (node *Node) sendHeartbeatTo(client rcppb.RCPClient, nodeId string, responsesChan chan<- *rcppb.AppendEntriesResponse ) {
	// log.Printf("Sending heartbeat to %s\n", nodeId)
	req, err := node.constructAppendEntriesRequest(node.currentTerm, nodeId)
	if err != nil {
		log.Printf("Error constructing AppendEntries Request: %v\n", err)
		return
	}

	if len(req.Entries) > 0 {
		log.Printf("Sending AppendEntries to %s: %v\n", nodeId, req)
	}
	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)
		return
	}

	if resp.Success {
		currNextIndex, _ := node.nextIndex.Load(nodeId)
		node.nextIndex.Store(nodeId, currNextIndex.(int64) + int64(len(req.Entries)))
		if len(req.Entries) > 0 {
			responsesChan <- resp
		}
		// log.Printf("Successful append entries to %s\n", nodeId)
	}
}

func countSuccessfulAppendEntries(responsesChan <-chan *rcppb.AppendEntriesResponse, timeout time.Duration) (int, int64) {
	waitTimer := time.After(timeout)
	successResponses := 0
	maxTerm := int64(0)
	for {
		select {
		case resp := <-responsesChan:
			successResponses += 1
			maxTerm = max(maxTerm, resp.Term)
		case <-waitTimer:
			return successResponses, maxTerm
		}
	}
}
