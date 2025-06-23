package node

import (
	"context"
	"log"
	"rcp/constants"
	"rcp/rcppb"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var printTimer bool

func randomElectionTimeout(nodeNum int) time.Duration {
	x := time.Duration(2000+nodeNum*500) * time.Millisecond
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
	counter := 0
	for {
		counter += 1
		// Send AppendEntry only if live and is leader
		if node.Live && node.isLeader {
			channelReadTimer := time.After(1500 * time.Microsecond)
			var logsToSend []*rcppb.LogEntry
		loop:
			for i := 1; ; {
				if len(logsToSend) > constants.MaxLogsPerAppendEntry {
					break
				}
				select {
				// read from the channel which has requests received from the client
				case c := <-node.logBufferChan:
					logEntry := c.LogEntry
					log.Printf("Read log %d from channel\n", i)
					logEntry.Term = node.currentTerm
					logsToSend = append(logsToSend, logEntry)
					ndx := node.lastIndex + int64(len(logsToSend))
					go node.indexToCallbackChannelMap.Store(ndx, c.CallbackChannel)
					i += 1
				case <-channelReadTimer:
					break loop
				}
			}

			// Call AppendEntries on leader
			begin1 := time.Now()
			resp, err := node.AppendEntries(context.Background(), &rcppb.AppendEntriesReq{
				Term:         node.currentTerm,
				LeaderId:     node.Id,
				PrevLogIndex: node.lastIndex,
				LeaderCommit: node.commitIndex,
				PrevLogTerm:  node.lastTerm,
				Entries:      logsToSend,
			})
			selfSuccess := false
			if err != nil {
				log.Printf("append entry to self failed: %v", err)
				return
			} else if resp.Success {
				selfSuccess = true
			} else {
				return
			}

			if len(logsToSend) > 0 {
				log.Printf("LOGX (%d) Time to self append entry (%d entries): %v", counter, len(logsToSend), time.Since(begin1))
			}

			// channel to collect all responses to AppendEntries
			responseChan := make(chan *rcppb.AppendEntriesResponse)
			begin2 := time.Now()
			// Send AppendEntries to all other nodes
			node.reachableSetLock.RLock()
			for nodeId := range node.reachableNodes {
				client, ok := node.ClientMap[nodeId]
				if !ok {
					continue
				}
				go node.sendHeartbeatTo(client, nodeId, responseChan)
			}
			node.reachableSetLock.RUnlock()

			if len(logsToSend) > 0 {
				log.Printf("LOGX (%d) Time to send heartbeats to others: %v", counter, time.Since(begin2))
			}

			begin3 := time.Now()

			waitTimer := time.After(5000 * time.Millisecond)
			var waitAfterCommit <-chan time.Time = make(chan time.Time)
			successResponses := 0
			if selfSuccess {
				successResponses = 1
			}
			// isDone := false
		successReadingLoop:
			for successResponses < len(node.ClientMap) {
				select {
				case resp := <-responseChan:
					successResponses += 1
					// if len(logsToSend) > 0 {
					// 	log.Printf("LOGX Success responses: %d, rep quorum: %d\n", successResponses, node.replicationQuorum)
					// }
					if resp.Term > node.currentTerm {
						node.currentTerm = resp.Term
						node.isLeader = false

						break successReadingLoop
					}

					if successResponses == node.replicationQuorum {
						prevCommit := node.commitIndex
						node.commitIndex = node.lastIndex
						go node.doCallbacks(prevCommit + 1, node.commitIndex)
						// log.Printf("waiter here: %v", time.Since(now))
						if node.isPersistent {
							waitAfterCommit = time.After(10 * time.Millisecond)
						} else {
							waitAfterCommit = time.After(100 * time.Microsecond)
						}
						
						if len(logsToSend) > 0 {
							log.Printf("LOGX (%d) Committed index %d, Setting shorter timer hopefully: %v, abs time: %v", counter, node.commitIndex, time.Since(begin3), time.Now().UnixMilli())
						}
					}
				case <-waitTimer:
					// log.Printf("Timer out")
					if len(logsToSend) > 0 {
						log.Printf("LOGX Breaking without quorum: %v Successes: %d", time.Since(begin3), successResponses)
					}
					break successReadingLoop
				case <-waitAfterCommit:
					if len(logsToSend) > 0 {
						log.Printf("LOGX Time waiting for extra commits: %v", time.Since(begin3))
					}
					break successReadingLoop
				}
			}

		}
		// time.Sleep(2 * time.Millisecond)
	}
}

func (node *Node) constructAppendEntriesRequest(term int64, nodeId string) (*rcppb.AppendEntriesReq, int64, error) {
	nextIndex, _ := node.nextIndex.Load(nodeId)
	var entries []*rcppb.LogEntry
	var err error
	if node.isPersistent {
		entries, err = node.db.GetLogsFromIndex(nextIndex.(int64))
		if err != nil {
			log.Printf("Error getting logs to construct append entries: %v", err)
			return nil, -1, err
		}
	} else {
		entries, err = node.GetInMemoryLogsFromIndex(nextIndex.(int64))
		if err != nil {
			log.Printf("Error getting logs to construct append entries: %v", err)
			return nil, -1, err
		}
	}
	
	prevLogTerm := int64(-1)
	if nextIndex.(int64)-1 >= 0 {
		if node.isPersistent {
			prevLogEntry, err := node.db.GetLogAtIndex(nextIndex.(int64) - 1)
			if err != nil {
				log.Printf("error fetching prevLogEntry %d for %s: %v", nextIndex.(int64)-1, nodeId, err)
				return nil, -1, err
			}
			prevLogTerm = prevLogEntry.Term
		} else {
			prevLogEntry, err := node.GetInMemoryLog(nextIndex.(int64) - 1)
			if err != nil {
				log.Printf("error fetching prevLogEntry %d for %s: %v", nextIndex.(int64)-1, nodeId, err)
				return nil, -1, err
			}
			prevLogTerm = prevLogEntry.Term
		}
		
	}
	// nextLogIndex, _ := node.nextIndex.Load(nodeId)
	delayRaw, ok := node.delays.Load(nodeId)
	var delay int64
	if !ok {
		delay = 0
	} else {
		delay = delayRaw.(int64)
	}
	if len(entries) > 0 {
		log.Printf("Append entries req to %s fromIndex: %d, number of entries: %d", nodeId, nextIndex, len(entries))
	}
	return &rcppb.AppendEntriesReq{
		Term:         term,
		LeaderId:     node.Id,
		PrevLogIndex: int64(nextIndex.(int64) - 1),
		LeaderCommit: node.commitIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		Delay:        int64(delay),
	}, nextIndex.(int64), nil
}

func (node *Node) sendHeartbeatTo(client rcppb.RCPClient, nodeId string, responsesChan chan *rcppb.AppendEntriesResponse) {
	// log.Printf("Sending heartbeat to %s\n", nodeId)
	req, currNextIndex, err := node.constructAppendEntriesRequest(node.currentTerm, nodeId)
	if err != nil {
		log.Printf("Error constructing AppendEntries Request: %v\n", err)
		return
	}

	if len(req.Entries) > 0 {
		log.Printf("Sending AppendEntries to %s with %d entries", nodeId, len(req.Entries))
	}
	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		if node.protocol == "rcp" {
			st, ok := status.FromError(err)
			log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)
			if ok && st.Code() == codes.Unavailable {
				node.checkInsertFailureLog(nodeId)
			}
		}
		return
	}
	if len(req.Entries) > 0 {
		log.Printf("Received append entries response from %s: %v", nodeId, resp)
	}
	if node.protocol == "rcp" {
		go node.checkInsertRecoveryLog(nodeId)
	}

	if resp.Success {

		if len(req.Entries) > 0 {
			// currNextIndex, _ := node.nextIndex.Load(nodeId)
			node.nextIndex.Store(nodeId, currNextIndex+int64(len(req.Entries)))
			// if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
			// 	responsesChan <- resp
			// 	// node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
			// }

		}
		if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
			// if len(req.Entries) > 0 {
			// 	log.Printf("LOGX Sending resp: %v to chan from %s", resp.Success, nodeId)
			// }

			responsesChan <- resp
			// node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
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
