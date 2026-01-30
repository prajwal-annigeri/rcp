package node

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"rcp/constants"
	"rcp/grpc/orcapb"
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
	delay := node.ElectionTimeoutMin
	if node.ElectionTimeoutMax > node.ElectionTimeoutMin {
		jitter := rand.Int63n(int64(node.ElectionTimeoutMax - node.ElectionTimeoutMin))
		delay = node.ElectionTimeoutMin + time.Duration(jitter)
	}
	node.electionTimer.Reset(delay)
}

func (node *Node) monitorElectionTimer() {
	for {
		<-node.electionTimer.C
		if !node.isLeader && node.Live {
			log.Println("Election timer ran out")
			node.requestVotes()
		}
		node.resetElectionTimer()
	}
}

func (node *Node) startReceiverLoop(entriesCh <-chan LogWithCallbackChannel) {
	// Batch count might not be accurate since heartbeat loop is running independently
	// and is not taken into account, but this is just an optimization
	var batchCount int
	timer := time.NewTimer(time.Hour) // idle

	for {
		select {
		case entry := <-entriesCh:
			node.mutex.Lock()

			// Add to own log if leader
			if node.isLeader {
				currIdx := node.AppendLogLocked(entry.LogEntry)
				node.indexToCallbackChannelMap[currIdx] = entry.CallbackChannel
				batchCount += 1

				// Start batch timeout when receive first request
				if batchCount == 1 {
					timer.Reset(node.BatchTimeout)
				}

				if batchCount >= node.BatchSizeLow {
					node.flushBatch()
					batchCount = 0
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}

			} else {
				entry.CallbackChannel <- CallbackReply{node.votedFor, ErrNotLeader}
			}

			node.mutex.Unlock()
		case <-timer.C:
			if batchCount > 0 {
				node.flushBatch()
				batchCount = 0
			}
		}
	}
}

func (node *Node) flushBatch() {
	log.Println("Flush batch called")
	for nodeId := range node.ClientMap {
		go node.sendHeartbeatTo(nodeId, false)
	}
}

func (node *Node) startHeartbeatLoop(nodeId string) {
	log.Printf("Starting a heartbeat loop for node %s", nodeId)

	backingOff := true
	retryCount := 0
	timer := time.NewTimer(0) // trigger immediately on start

	defer timer.Stop()

	for {
		select {
		case <-node.stepdownChan:
			return // Stop the loop if node steps down

		case <-timer.C:
			success, err := node.sendHeartbeatTo(nodeId, backingOff)

			if err != nil {
				if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNotAlive) || errors.Is(err, ErrTooManyInFlightMessages) {
					continue
				}

				if node.protocol == "rcp" {
					st, ok := status.FromError(err)
					log.Printf("Received error response from %s to heartbeat: %v\n", nodeId, err)

					// If not denied because of outdated term, count as failure
					if ok && st.Code() != codes.Aborted {
						retryCount += 1

						// Too many retries, failure detected
						if retryCount > constants.FailureRetryCount {
							node.mutex.Lock()
							if _, failed := node.failedSet[nodeId]; !failed {
								if _, pendingFailure := node.pendingFailureSet[nodeId]; !pendingFailure {
									log.Printf("Failure detected on %s", nodeId)
									// If node is not failed nor pending failure, failure detected
									failureLog := &orcapb.LogEntry{
										LogType: orcapb.LogType_FAILURE,
										NodeId:  nodeId,
										Term:    node.currentTerm,
									}
									node.AppendLogLocked(failureLog)
								}
							}
							node.mutex.Unlock()
						}
					}
				}
				timer.Reset(node.HeartbeatTimeout)
				continue
			}

			retryCount = 0

			if success {
				// No longer backing off after first success
				backingOff = false
			}

			if !success && backingOff {
				timer.Reset(0)
				continue
			}

			timer.Reset(node.HeartbeatTimeout)
		}
	}
}

func (node *Node) sendHeartbeatTo(nodeId string, backingOff bool) (bool, error) {
	node.mutex.Lock()

	if !node.isLeader {
		node.mutex.Unlock()
		return false, ErrNotLeader
	}

	if !node.Live {
		node.mutex.Unlock()
		return false, ErrNotAlive
	}

	if node.inFlightMessageCount[nodeId] >= constants.MaxInFlightMessageCount {
		log.Printf("Too many in flight messages for %s: %d", nodeId, node.inFlightMessageCount[nodeId])
		node.mutex.Unlock()
		return false, ErrTooManyInFlightMessages
	}

	begin := time.Now()

	// Build AppendEntries
	nextIndex := node.nextIndex[nodeId]

	entries, err := node.db.GetLogsFromIndex(nextIndex, node.BatchSizeHigh)
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

	req := &orcapb.AppendEntriesRequest{
		Term:         node.currentTerm,
		LeaderId:     node.Id,
		PrevLogIndex: nextIndex - 1,
		LeaderCommit: node.commitIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}

	node.inFlightMessageCount[nodeId] += 1
	node.mutex.Unlock()

	// Send AppendEntries
	if len(req.Entries) > 0 {
		log.Printf("Sending AppendEntries to %s with %d entries from index %d after %v", nodeId, len(req.Entries), nextIndex, time.Since(begin))
	}

	client := node.ClientMap[nodeId]
	resp, err := client.AppendEntries(context.Background(), req)

	if len(req.Entries) > 0 {
		log.Printf("Received AppendEntries ack from %s after %v", nodeId, time.Since(begin))
	}

	node.mutex.Lock()
	node.inFlightMessageCount[nodeId] -= 1

	if err != nil {
		node.mutex.Unlock()
		return false, err
	}

	if node.protocol == "rcp" {
		// If node failed, move it to pending recovery if not yet there already
		if _, failed := node.failedSet[nodeId]; failed {
			if _, pendingRecovery := node.pendingRecoverySet[nodeId]; !pendingRecovery {
				recoveryLog := &orcapb.LogEntry{
					LogType: orcapb.LogType_RECOVERY,
					NodeId:  nodeId,
					Term:    node.currentTerm,
				}
				node.AppendLogLocked(recoveryLog)
			}
		}
	}

	if resp.Success {
		if len(req.Entries) > 0 {
			node.nextIndex[nodeId] = nextIndex + int64(len(req.Entries))
			node.matchIndex[nodeId] = node.nextIndex[nodeId] - 1

			// Calculate replication
			commitIndex := node.commitIndex
			nodeRequired := node.replicationQuorum - 1
			matchIdx, ok := quorumMatchIndex(node.matchIndex, node.failedSet, node.pendingRecoverySet, nodeRequired)
			if ok && matchIdx > commitIndex {
				node.commitIndex = matchIdx
				node.executeUntilLocked(node.commitIndex)
			}
		}
	} else {
		// TODO: Should this be here?
		if resp.Term > node.currentTerm {
			node.currentTerm = resp.Term
			node.StepDownLocked()
		} else {
			if node.nextIndex[nodeId] > node.BackoffDec {
				node.nextIndex[nodeId] -= node.BackoffDec
			} else {
				node.nextIndex[nodeId] = 0
			}
		}
	}

	// log.Printf("Finished heartbeat to %s after %v", nodeId, time.Since(begin))
	node.mutex.Unlock()
	return resp.Success, nil
}
