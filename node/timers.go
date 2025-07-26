package node

import (
	"context"
	"log"
	"math/rand"
	"rcp/constants"
	"rcp/rcppb"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (node *Node) resetElectionTimer() {
	// Stop and drain channel to prevent race bug
	if !node.electionTimer.Stop() {
		select {
		case <-node.electionTimer.C:
		default:
		}
	}
	node.electionTimer.Reset(time.Duration(300+rand.Intn(400)) * time.Millisecond)
}

func (node *Node) monitorElectionTimer() {
	for {
		<-node.electionTimer.C
		if !node.isLeader && node.Live {
			log.Println("Election timer ran out")
			node.requestVotes()
		}
		// log.Println("Reset election timer")
		node.resetElectionTimer()
	}
}

func (node *Node) startReceiverLoop(entriesCh <-chan LogWithCallbackChannel) {
	for entry := range entriesCh {
		node.mutex.Lock()

		// Add to own log if leader
		if node.isLeader {
			// Handle failure/recovery log
			currIdx := node.AppendLog(entry.LogEntry)
			node.indexToCallbackChannelMap[currIdx] = entry.CallbackChannel
		} else {
			entry.CallbackChannel <- CallbackReply{node.votedFor, ErrNotLeader}
		}

		node.mutex.Unlock()
	}
}

func (node *Node) startHeartbeatLoop(nodeId string) {
	log.Printf("Starting a heartbeat loop for node %s", nodeId)

	backingOff := false
	retryCount := 0
	timer := time.NewTimer(0) // trigger immediately on start
	begin := time.Now()

	defer timer.Stop()

	for {
		select {
		case <-node.stepdownChan:
			return // Stop the loop if node steps down

		case <-timer.C:
			node.mutex.Lock()
			if !node.isLeader || !node.Live {
				node.mutex.Unlock()
				continue
			}

			nextIndex := node.nextIndex[nodeId]
			log.Printf("Building AppendEntries for %s after %v", nodeId, time.Since(begin))

			// Build AppendEntries
			entries, err := node.db.GetLogsFromIndex(nextIndex, node.BatchSize)
			if err != nil {
				log.Panicf("Error getting logs from index %d: %v", nextIndex, err)
			}

			var prevLogTerm int64
			if nextIndex != 0 {
				prevLog, err := node.db.GetLogAtIndex(nextIndex - 1)
				if err != nil {
					log.Panicf("Error getting prev log index %d: %v", nextIndex-1, err)
				}
				prevLogTerm = prevLog.Term
			} else {
				// Handle first log
				prevLogTerm = 0
			}

			req := &rcppb.AppendEntriesReq{
				Term:         node.currentTerm,
				LeaderId:     node.Id,
				PrevLogIndex: nextIndex - 1,
				LeaderCommit: node.commitIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				// Delay:        int64(delay),
			}

			node.mutex.Unlock()

			// Send AppendEntries
			if len(req.Entries) > 0 {
				log.Printf("Sending AppendEntries to %s with %d entries", nodeId, len(req.Entries))
			}
			log.Printf("Sending AppendEntries for %s after %v", nodeId, time.Since(begin))

			client := node.ClientMap[nodeId]
			resp, err := client.AppendEntries(context.Background(), req)

			log.Printf("Taking back lock for %s after %v", nodeId, time.Since(begin))
			node.mutex.Lock()
			log.Printf("Getting back lock for %s after %v", nodeId, time.Since(begin))

			if err != nil {
				if node.protocol == "rcp" {
					st, ok := status.FromError(err)
					log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)

					// If not denied because of outdated term, count as failure
					if ok && st.Code() != codes.Aborted {
						retryCount += 1

						// Too many retries, failure detected
						if retryCount > constants.FailureRetryCount {
							if _, failed := node.failedSet[nodeId]; !failed {
								if _, pendingFailure := node.pendingFailureSet[nodeId]; !pendingFailure {
									// If node is not failed nor pending failure, failure detected
									failureLog := &rcppb.LogEntry{
										LogType: rcppb.LogType_FAILURE,
										NodeId:  nodeId,
										Term:    node.currentTerm,
									}
									node.AppendLog(failureLog)
								}
							}
						}
					}
				}
				timer.Reset(constants.HeartbeatIntervalMillisecond * time.Millisecond)
				node.mutex.Unlock()
				continue
			}

			retryCount = 0
			if len(req.Entries) > 0 {
				log.Printf("Received append entries response from %s: %v", nodeId, resp)
			}
			log.Printf("Received AppendEntries response from %s after %v", nodeId, time.Since(begin))

			if node.protocol == "rcp" {
				// If node failed, move it to pending recovery if not yet there already
				if _, failed := node.failedSet[nodeId]; failed {
					if _, pendingRecovery := node.pendingRecoverySet[nodeId]; !pendingRecovery {
						recoveryLog := &rcppb.LogEntry{
							LogType: rcppb.LogType_RECOVERY,
							NodeId:  nodeId,
							Term:    node.currentTerm,
						}
						node.AppendLog(recoveryLog)
					}
				}
			}

			if resp.Success {
				if len(req.Entries) > 0 {
					node.nextIndex[nodeId] = nextIndex + int64(len(req.Entries))
					node.matchIndex[nodeId] = node.nextIndex[nodeId] - 1
					// if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
					// 	responsesChan <- resp
					// 	// node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
					// }

					// Calculate replication
					sortedMatchIndex := SortMapByValueDescending(node.matchIndex)
					commitIndex := node.commitIndex
					nodeRequired := node.replicationQuorum - 1

					// TODO: Handle if pending failure node recovers
					for _, nodeIdMatchIndexPair := range sortedMatchIndex {
						if _, failed := node.failedSet[nodeIdMatchIndexPair.Key]; failed {
							continue
						}

						if _, pendingFailure := node.failedSet[nodeIdMatchIndexPair.Key]; pendingFailure {
							continue
						}

						nodeRequired -= 1

						if nodeRequired <= 0 {
							if nodeIdMatchIndexPair.Value > commitIndex {
								node.commitIndex = nodeIdMatchIndexPair.Value
								node.executeUntil(node.commitIndex)
							}
						}
					}
				}

				// No longer backing off after first success
				backingOff = false

				// If there are still logs to replicate, use faster interval
				if node.nextIndex[nodeId] > node.GetLastIndex() {
					timer.Reset(constants.HeartbeatIntervalMillisecond * time.Millisecond)
				} else {
					timer.Reset(0)
				}
			} else {
				if resp.Term > node.currentTerm {
					node.currentTerm = resp.Term
					node.StepDown()
				} else {
					if backingOff {
						node.nextIndex[nodeId] -= 1
						timer.Reset(0)
					} else {
						// Retry
						timer.Reset(constants.HeartbeatIntervalMillisecond * time.Millisecond)
					}
				}
			}

			log.Printf("Reached the end for %s after %v", nodeId, time.Since(begin))
			begin = time.Now()
			node.mutex.Unlock()
		}
	}
}

// // function to send heartbeats. Will be running as a goroutine in the background.
// func (node *Node) sendHeartbeats() {
// 	counter := 0
// 	heartbeatCounter := 5

// 	for {
// 		counter += 1
// 		// Send AppendEntry only if live and is leader
// 		if node.Live && node.isLeader {
// 			channelReadTimer := time.After(10 * time.Millisecond)
// 			var logsToSend []*rcppb.LogEntry
// 		loop:
// 			for i := 1; ; {
// 				if len(logsToSend) > node.BatchSize {
// 					break
// 				}
// 				select {
// 				// read from the channel which has requests received from the client
// 				case c := <-node.logBufferChan:
// 					logEntry := c.LogEntry
// 					log.Printf("Read log %d from channel\n", i)
// 					logEntry.Term = node.currentTerm
// 					logsToSend = append(logsToSend, logEntry)
// 					ndx := node.lastIndex + int64(len(logsToSend))
// 					go node.indexToCallbackChannelMap.Store(ndx, c.CallbackChannel)
// 					i += 1
// 				case <-channelReadTimer:
// 					break loop
// 				}
// 			}

// 			// Send heartbeat only after a few empty AppendEntries
// 			if len(logsToSend) == 0 {
// 				heartbeatCounter -= 1

// 				if heartbeatCounter > 0 {
// 					continue
// 				}
// 			}

// 			log.Println("Sending AppendEntries")
// 			node.mutex.Lock()
// 			defer node.mutex.Unlock()

// 			heartbeatCounter = 5

// 			// Call AppendEntries on leader
// 			begin1 := time.Now()
// 			resp, err := node.AppendEntries(context.Background(), &rcppb.AppendEntriesReq{
// 				Term:         node.currentTerm,
// 				LeaderId:     node.Id,
// 				PrevLogIndex: node.lastIndex,
// 				LeaderCommit: node.commitIndex,
// 				PrevLogTerm:  node.lastTerm,
// 				Entries:      logsToSend,
// 			})
// 			selfSuccess := false
// 			if err != nil {
// 				log.Printf("append entry to self failed: %v", err)
// 				return
// 			} else if resp.Success {
// 				selfSuccess = true
// 			} else {
// 				return
// 			}

// 			if len(logsToSend) > 0 {
// 				log.Printf("LOGX (%d) Time to self append entry (%d entries): %v", counter, len(logsToSend), time.Since(begin1))
// 			}

// 			// channel to collect all responses to AppendEntries
// 			responseChan := make(chan *rcppb.AppendEntriesResponse)
// 			begin2 := time.Now()
// 			// Send AppendEntries to all other nodes
// 			for nodeId, client := range node.ClientMap {
// 				go node.sendHeartbeatTo(client, nodeId, responseChan)
// 			}

// 			if len(logsToSend) > 0 {
// 				log.Printf("LOGX (%d) Time to send heartbeats to others: %v", counter, time.Since(begin2))
// 			}

// 			begin3 := time.Now()

// 			waitTimer := time.After(5000 * time.Millisecond)
// 			// var waitAfterCommit <-chan time.Time = make(chan time.Time)
// 			successResponses := 0
// 			if selfSuccess {
// 				successResponses = 1
// 			}
// 			// isDone := false
// 		successReadingLoop:
// 			for successResponses < len(node.ClientMap) {
// 				select {
// 				case resp := <-responseChan:
// 					successResponses += 1
// 					// if len(logsToSend) > 0 {
// 					// 	log.Printf("LOGX Success responses: %d, rep quorum: %d\n", successResponses, node.replicationQuorum)
// 					// }
// 					if resp.Term > node.currentTerm {
// 						node.currentTerm = resp.Term
// 						node.isLeader = false

// 						break successReadingLoop
// 					}

// 					if successResponses == node.replicationQuorum {
// 						prevCommit := node.commitIndex
// 						node.commitIndex = node.lastIndex
// 						go node.doCallbacks(prevCommit+1, node.commitIndex)
// 						err := node.executeUntil(node.commitIndex)
// 						if err != nil {
// 							log.Printf("Error executing: %v", err)
// 						}

// 						if len(logsToSend) > 0 {
// 							log.Printf("LOGX (%d) Committed index %d, finishes in: %v, abs time: %v", counter, node.commitIndex, time.Since(begin3), time.Now().UnixMilli())
// 						}

// 						break successReadingLoop
// 					}
// 				case <-waitTimer:
// 					log.Printf("Timer out")
// 					if len(logsToSend) > 0 {
// 						log.Printf("LOGX Breaking without quorum: %v Successes: %d", time.Since(begin3), successResponses)
// 					}
// 					break successReadingLoop
// 					// case <-waitAfterCommit:
// 					// 	if len(logsToSend) > 0 {
// 					// 		log.Printf("LOGX Time waiting for extra commits: %v", time.Since(begin3))
// 					// 	}
// 					// 	break successReadingLoop
// 				}
// 			}

// 		}
// 		// time.Sleep(2 * time.Millisecond)
// 	}
// }

// func (node *Node) constructAppendEntriesRequest(term int64, nodeId string) (*rcppb.AppendEntriesReq, int64, error) {
// 	nextIndex, _ := node.nextIndex.Load(nodeId)
// 	var entries []*rcppb.LogEntry
// 	var err error
// 	entries, err = node.db.GetLogsFromIndex(nextIndex.(int64))
// 	if err != nil {
// 		log.Printf("Error getting logs to construct append entries: %v", err)
// 		return nil, -1, err
// 	}

// 	prevLogTerm := int64(-1)
// 	if nextIndex.(int64)-1 >= 0 {
// 		prevLogEntry, err := node.db.GetLogAtIndex(nextIndex.(int64) - 1)
// 		if err != nil {
// 			log.Printf("error fetching prevLogEntry %d for %s: %v", nextIndex.(int64)-1, nodeId, err)
// 			return nil, -1, err
// 		}
// 		prevLogTerm = prevLogEntry.Term

// 	}
// 	// nextLogIndex, _ := node.nextIndex.Load(nodeId)
// 	// delayRaw, ok := node.delays.Load(nodeId)
// 	// var delay int64
// 	// if !ok {
// 	// 	delay = 0
// 	// } else {
// 	// 	delay = delayRaw.(int64)
// 	// }
// 	if len(entries) > 0 {
// 		log.Printf("Append entries req to %s fromIndex: %d, number of entries: %d", nodeId, nextIndex, len(entries))
// 	}
// 	return &rcppb.AppendEntriesReq{
// 		Term:         term,
// 		LeaderId:     node.Id,
// 		PrevLogIndex: int64(nextIndex.(int64) - 1),
// 		LeaderCommit: node.commitIndex,
// 		PrevLogTerm:  prevLogTerm,
// 		Entries:      entries,
// 		// Delay:        int64(delay),
// 	}, nextIndex.(int64), nil
// }

// func (node *Node) sendHeartbeatTo(client rcppb.RCPClient, nodeId string, responsesChan chan *rcppb.AppendEntriesResponse) {
// 	// log.Printf("Sending heartbeat to %s\n", nodeId)
// 	req, currNextIndex, err := node.constructAppendEntriesRequest(node.currentTerm, nodeId)
// 	if err != nil {
// 		log.Printf("Error constructing AppendEntries Request: %v\n", err)
// 		return
// 	}

// 	if len(req.Entries) > 0 {
// 		log.Printf("Sending AppendEntries to %s with %d entries", nodeId, len(req.Entries))
// 	}
// 	resp, err := client.AppendEntries(context.Background(), req)
// 	if err != nil {
// 		if node.protocol == "rcp" {
// 			st, ok := status.FromError(err)
// 			log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)
// 			if ok && st.Code() == codes.Unavailable {
// 				node.checkInsertFailureLog(nodeId)
// 			}
// 		}
// 		return
// 	}
// 	if len(req.Entries) > 0 {
// 		log.Printf("Received append entries response from %s: %v", nodeId, resp)
// 	}
// 	if node.protocol == "rcp" {
// 		go node.checkInsertRecoveryLog(nodeId)
// 	}

// 	if resp.Success {

// 		if len(req.Entries) > 0 {
// 			// currNextIndex, _ := node.nextIndex.Load(nodeId)
// 			node.nextIndex.Store(nodeId, currNextIndex+int64(len(req.Entries)))
// 			// if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
// 			// 	responsesChan <- resp
// 			// 	// node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
// 			// }

// 		}
// 		if status, _ := node.serverStatusMap.Load(nodeId); status.(bool) {
// 			// if len(req.Entries) > 0 {
// 			// 	log.Printf("LOGX Sending resp: %v to chan from %s", resp.Success, nodeId)
// 			// }

// 			responsesChan <- resp
// 			// node.increaseReplicationCount(req.PrevLogIndex + int64(len(req.Entries)))
// 		}
// 		// log.Printf("Successful append entries to %s\n", nodeId)
// 	} else {
// 		if resp.Term > node.currentTerm {
// 			node.currentTerm = resp.Term
// 			node.isLeader = false
// 		} else {
// 			currNextIndex, _ := node.nextIndex.Load(nodeId)
// 			node.nextIndex.Store(nodeId, currNextIndex.(int64)-1)
// 		}

// 	}
// }

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
