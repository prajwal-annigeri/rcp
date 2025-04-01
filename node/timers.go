package node

import (
	"context"
	"log"
	"rcp/rcppb"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var printTimer bool

func randomElectionTimeout(nodeNum int) time.Duration {
	x := time.Duration(700+nodeNum*250) * time.Millisecond
	// x := time.Duration(250 + rand.Intn(2500)) * time.Millisecond
	if !printTimer {
		log.Println(x)
		printTimer = true
	}
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

// function to send heartbeats. Will be running as a goroutine in the background.
func (node *Node) sendHeartbeats() {

	for {
		// Send AppendEntry only if live and is leader
		if node.Live && node.isLeader {
			doneReading := false
			var logsToSend []*rcppb.LogEntry
			for {
				select {
				// read from the channel which has requests received from the client
				case c := <-node.logBufferChan:
					log.Printf("Read log from channel: %v\n", c)
					c.Term = node.currentTerm
					// node.replicatedCount.Store(c.)
					logsToSend = append(logsToSend, c)
				default:
					doneReading = true
				}
				if doneReading {
					break
				}
			}

			// Call AppendEntries on leader
			resp, err := node.AppendEntries(context.Background(), &rcppb.AppendEntriesReq{
				Term:         node.currentTerm,
				LeaderId:     node.Id,
				PrevLogIndex: node.lastIndex,
				LeaderCommit: node.commitIndex,
				PrevLogTerm:  node.lastTerm,
				Entries:      logsToSend,
			})
			// selfSuccess := false
			if err != nil {
				log.Printf("append entry to self failed: %v", err)
				return
			} else if resp.Success {
				// selfSuccess = true
				node.replicatedCount.Store(node.lastIndex, 1)
			} else {
				return
			}

			// channel to collect all responses to AppendEntries
			// responseChan := make(chan *rcppb.AppendEntriesResponse)

			// Send AppendEntries to all other nodes
			for nodeId, client := range node.ClientMap {
				go node.sendHeartbeatTo(client, nodeId)
			}

			// successResponses, maxTerm := countSuccessfulAppendEntries(responseChan, 100*time.Millisecond)
			// if selfSuccess {
			// 	successResponses += 1
			// }
			// if successResponses >= node.K+1 {
			// 	node.commitIndex = node.lastIndex
			// } else if maxTerm > node.currentTerm {
			// 	node.currentTerm = maxTerm
			// 	node.isLeader = false
			// }

		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (node *Node) constructAppendEntriesRequest(term int64, nodeId string) (*rcppb.AppendEntriesReq, error) {
	nextIndex, _ := node.nextIndex.Load(nodeId)
	entries, err := node.db.GetLogsFromIndex(nextIndex.(int64))
	prevLogTerm := int64(-1)
	if nextIndex.(int64)-1 >= 0 {
		prevLogEntry, err := node.db.GetLogAtIndex(nextIndex.(int64) - 1)
		if err != nil {
			log.Printf("error fetching prevLogEntry: %v", err)
			return nil, err
		}
		prevLogTerm = prevLogEntry.Term
	}
	if err != nil {
		return nil, err
	}
	nextLogIndex, _ := node.nextIndex.Load(nodeId)
	delayRaw, ok := node.delays.Load(nodeId)
	var delay int
	if !ok {
		delay = 0
	} else {
		delay = delayRaw.(int)
	}
	return &rcppb.AppendEntriesReq{
		Term:         term,
		LeaderId:     node.Id,
		PrevLogIndex: int64(nextLogIndex.(int64) - 1),
		LeaderCommit: node.commitIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		Delay:        int64(delay),
	}, nil
}

func (node *Node) sendHeartbeatTo(client rcppb.RCPClient, nodeId string) {
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
		st, ok := status.FromError(err)
		log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)
		if ok && st.Code() == codes.Unavailable {
			node.checkInsertFailureLog(nodeId)
		}
		return
	}
	if len(req.Entries) > 0 {
		log.Printf("Received appendentries response from %s: %v", nodeId, resp)
	}

	go node.checkInsertRecoveryLog(nodeId)
	if resp.Success {
		currNextIndex, _ := node.nextIndex.Load(nodeId)
		node.nextIndex.Store(nodeId, currNextIndex.(int64)+int64(len(req.Entries)))
		if len(req.Entries) > 0 {
			// responsesChan <- resp
			if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
				node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
			}

		}
		// log.Printf("Successful append entries to %s\n", nodeId)
	} else {
		if resp.Term > node.currentTerm {
			node.currentTerm = resp.Term
			node.isLeader = false
		} else {
			currNextIndex, _ := node.nextIndex.Load(nodeId)
			node.nextIndex.Store(nodeId, currNextIndex.(int64)-1)
		}

	}
}

// func countSuccessfulAppendEntries(responsesChan <-chan *rcppb.AppendEntriesResponse, timeout time.Duration) (int, int64) {
// 	waitTimer := time.After(timeout)
// 	successResponses := 0
// 	maxTerm := int64(0)
// 	for {
// 		select {
// 		case resp := <-responsesChan:
// 			successResponses += 1
// 			maxTerm = max(maxTerm, resp.Term)
// 		case <-waitTimer:
// 			return successResponses, maxTerm
// 		}
// 	}
// }
