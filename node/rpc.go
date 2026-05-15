package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/constants"
	"rcp/rcppb"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (node *Node) AppendEntries(ctx context.Context, appendEntryReq *rcppb.AppendEntriesReq) (*rcppb.AppendEntriesResponse, error) {
	if !node.Live {
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Unavailable, "not alive")
	}

	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Received AppendEntries from %s with %d entries at term %d and prev index %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term, appendEntryReq.PrevLogIndex)
	}

	node.mutex.Lock()
	defer node.mutex.Unlock()

	begin := time.Now()

	if appendEntryReq.Term < node.currentTerm {
		log.Printf("Denying append because my term %d is > %d\n", node.currentTerm, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Aborted, fmt.Sprintf("%s denied append because its term is %d which is greater than %d", node.Id, node.currentTerm, appendEntryReq.Term))
	}

	// Update term and convert to follower if needed
	if node.currentTerm < appendEntryReq.Term {
		node.currentTerm = appendEntryReq.Term
		node.StepDownLocked()
	}

	// log.Println("Reset election timer")
	node.resetElectionTimer()

	// Check if node is outdated
	if appendEntryReq.PrevLogIndex >= 0 {
		prevLogTerm := int64(-1)
		logEntry, err := node.db.GetLogAtIndex(appendEntryReq.PrevLogIndex)

		if err == nil {
			prevLogTerm = logEntry.Term
		}

		if prevLogTerm != appendEntryReq.PrevLogTerm {
			log.Printf("Denying append entry because prev log entry term does not match, mine: %d, in req: %d", prevLogTerm, appendEntryReq.PrevLogTerm)
			return &rcppb.AppendEntriesResponse{
				Term:    node.currentTerm,
				Success: false,
			}, nil
		}
	}

	// Append any new entries
	err := node.insertLogsLocked(appendEntryReq)
	if err != nil {
		log.Panic("Error inserting logs")
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Internal, err.Error())
	}

	// Update commit index
	if appendEntryReq.LeaderCommit > node.commitIndex {
		node.commitIndex = min(appendEntryReq.LeaderCommit, node.GetLastIndexLocked())
		err = node.executeUntilLocked(node.commitIndex)
		if err != nil {
			log.Printf("Error executing: %v", err)
		}
	}

	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Time for appendEntries with %d entries: %v", len(appendEntryReq.Entries), time.Since(begin))
	}

	return &rcppb.AppendEntriesResponse{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}

// This function assume mutex is already locked
func (node *Node) insertLogsLocked(appendEntryReq *rcppb.AppendEntriesReq) error {
	if len(appendEntryReq.Entries) == 0 {
		return nil
	}

	currIndex := appendEntryReq.PrevLogIndex + 1
	// lastEntryTerm := appendEntryReq.Term

	for _, entry := range appendEntryReq.Entries {
		node.InsertLogLocked(entry, currIndex)

		// // Lookup existing log at index

		// if _, ok := node.possibleFailureOrRecoveryIndex.Load(currIndex); ok {
		// 	existingEntry, err := node.db.GetLogAtIndex(currIndex)
		// 	if err == nil {
		// 		if existingEntry.LogType == "failure" {
		// 			node.removeFromFailureSet(existingEntry.NodeId)

		// 			// node.failureSetLock.Lock()
		// 			// defer node.failureSetLock.Unlock()
		// 			// delete(node.failureLogWaitingSet, nodeId)

		// 		} else if existingEntry.LogType == "recovery" {
		// 			node.removeFromRecoverySet(existingEntry.NodeId)

		// 			// node.recoverySetLock.Lock()
		// 			// defer node.recoverySetLock.Unlock()
		// 			// delete(node.recoveryLogWaitingSet, nodeId)

		// 		}
		// 	}
		// }

		// // Put new entry
		// if entry.LogType == "failure" || entry.LogType == "success" {
		// 	go node.possibleFailureOrRecoveryIndex.Store(currIndex, "")
		// }

		// err := node.db.PutLogAtIndex(currIndex, entry)
		// if err != nil {
		// 	log.Printf("Error putting log at index %d: %v", currIndex, err)
		// }

		// node.lastIndex = currIndex
		// lastEntryTerm = entry.Term
		currIndex++
	}

	// node.lastTerm = lastEntryTerm
	return nil
}

func (node *Node) RequestVote(ctx context.Context, requestVoteReq *rcppb.RequestVoteReq) (*rcppb.RequestVoteResponse, error) {
	if !node.Live {
		log.Printf("Received RequestVote: %s Term: %d I'm not alive", requestVoteReq.CandidateId, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.Unavailable, "not alive")
	}

	log.Printf("Received RequestVote from %s: Term %d", requestVoteReq.CandidateId, requestVoteReq.Term)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Reject if term is lower
	if requestVoteReq.Term < node.currentTerm {
		log.Printf("Denying vote to %s as my term is greater", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If term is higher, step down if the leader, update term
	if requestVoteReq.Term > node.currentTerm {
		log.Printf("Receive RequestVote with higher term from %s", requestVoteReq.CandidateId)
		node.currentTerm = requestVoteReq.Term
		node.StepDownLocked()
		node.votedFor = ""
	}

	// Check if already voted
	if node.votedFor != "" && node.votedFor != requestVoteReq.CandidateId {
		log.Printf("Already voted for %s in term %d", node.votedFor, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.PermissionDenied, fmt.Sprintf("already voted for %s for term %d", node.votedFor, requestVoteReq.Term))
	}

	// Check if candidate is up to date
	if requestVoteReq.LastLogTerm < node.GetLastTermLocked() || (requestVoteReq.LastLogTerm == node.GetLastTermLocked() && node.GetLastIndexLocked() > requestVoteReq.LastLogIndex) {
		log.Printf("Denying vote to %s as I have a more complete log", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if node.protocol == "raft" && !node.isElectionVoterLocked(requestVoteReq.CandidateId) {
		log.Printf("Denying vote to %s because it is not a voter in current configuration", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	log.Printf("Voting for %s for term %d\n", requestVoteReq.CandidateId, requestVoteReq.Term)
	node.currentTerm = requestVoteReq.Term
	node.votedFor = requestVoteReq.CandidateId
	node.StepDownLocked()

	return &rcppb.RequestVoteResponse{
		Term:        node.currentTerm,
		VoteGranted: true,
	}, nil
}

// func (node *Node) SetStatus(ctx context.Context, req *wrapperspb.BoolValue) (*wrapperspb.BoolValue, error) {
// 	log.Printf("Setting status: %t", req.Value)

// 	node.mutex.Lock()
// 	defer node.mutex.Unlock()

// 	if req.Value {
// 		node.StepDown()
// 	}
// 	node.Live = req.Value

// 	return &wrapperspb.BoolValue{Value: true}, nil
// }

// func (node *Node) Partition(ctx context.Context, req *rcppb.PartitionReq) (*wrapperspb.BoolValue, error) {
// 	node.reachableSetLock.Lock()
// 	defer node.reachableSetLock.Unlock()
// 	node.reachableNodes = make(map[string]struct{})
// 	for _, nodeId := range req.ReachableNodes {
// 		node.reachableNodes[nodeId] = struct{}{}
// 	}

// 	return &wrapperspb.BoolValue{Value: true}, nil
// }

// func (node *Node) Delay(ctx context.Context, req *rcppb.DelayRequest) (*wrapperspb.BoolValue, error) {
// 	to := req.NodeId
// 	delay := req.Delay

// 	if delay <= 0 || to == "" {
// 		return nil, errors.New("invalid request argument")
// 	}

// 	log.Printf("Setting delay: To=%s, Delay=%d", to, delay)

// 	node.delays.Store(to, delay)
// 	return &wrapperspb.BoolValue{Value: true}, nil
// }

func (node *Node) Healthz(ctx context.Context, req *rcppb.HealthzRequest) (*wrapperspb.BoolValue, error) {
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) Reconfigure(ctx context.Context, req *rcppb.ReconfigureRequest) (*rcppb.ClientResponse, error) {
	if !node.Live {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_ALIVE,
		}, nil
	}

	node.mutex.Lock()

	if node.protocol != "raft" {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_SUPPORTED,
			Value:   "reconfiguration is supported only when --protocol=raft",
		}, nil
	}

	if node.reconfigMode == ReconfigModeNone {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_SUPPORTED,
			Value:   "reconfiguration is disabled; set --reconfig-mode=joint|recraft|orca",
		}, nil
	}

	if !node.isLeader {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_LEADER,
			Value:   node.votedFor,
		}, nil
	}

	if node.reconfigInFlight {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
			Value:   "reconfiguration already in flight",
		}, nil
	}

	if len(req.VoterIds) == 0 {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
			Value:   "empty target voter set",
		}, nil
	}

	seen := make(map[string]struct{}, len(req.VoterIds))
	targetVoterSet := make(map[string]struct{}, len(req.VoterIds))
	for _, nodeID := range req.VoterIds {
		if nodeID == "" {
			node.mutex.Unlock()
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_BAD_REQUEST,
				Value:   "target voter set contains empty node ID",
			}, nil
		}

		if _, exists := seen[nodeID]; exists {
			node.mutex.Unlock()
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_BAD_REQUEST,
				Value:   "target voter set contains duplicate node IDs",
			}, nil
		}
		seen[nodeID] = struct{}{}
		targetVoterSet[nodeID] = struct{}{}

		if _, exists := node.knownNodeSet[nodeID]; !exists {
			node.mutex.Unlock()
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_BAD_REQUEST,
				Value:   fmt.Sprintf("unknown node ID in target voter set: %s", nodeID),
			}, nil
		}
	}

	if sameSet(node.activeVoterSet, targetVoterSet) {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: true,
			Value:   "target voter set matches current configuration",
		}, nil
	}

	entries, epoch, err := node.buildReconfigLogEntriesLocked(targetVoterSet)
	if err != nil {
		node.mutex.Unlock()
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_UNEXPECTED,
			Value:   err.Error(),
		}, nil
	}

	finalCallbackCh := make(chan CallbackReply, 1)

	for i, entry := range entries {
		idx := node.AppendLogLocked(entry)
		if i == len(entries)-1 {
			node.indexToCallbackChannelMap[idx] = finalCallbackCh
		}
	}

	node.reconfigInFlight = true
	node.reconfigEpoch = max(node.reconfigEpoch, epoch)

	node.flushBatch()
	node.mutex.Unlock()

	select {
	case callback := <-finalCallbackCh:
		if callback.Error != nil {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_UNEXPECTED,
				Value:   callback.Error.Error(),
			}, nil
		}

		return &rcppb.ClientResponse{
			Success: true,
			Value:   fmt.Sprintf("reconfiguration committed at epoch %d", epoch),
		}, nil
	case <-time.After(node.ConsensusTimeout):
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_TIMEOUT,
			Value:   "timed out waiting for reconfiguration commit",
		}, nil
	}
}

func (node *Node) Store(ctx context.Context, req *rcppb.StoreRequest) (*rcppb.ClientResponse, error) {
	if !node.Live {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_ALIVE,
		}, nil
	}

	if req.Key == "" {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
			Value:   "missing key",
		}, nil
	}

	if req.Value == "" {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
			Value:   "missing value",
		}, nil
	}

	if req.Bucket == "" {
		req.Bucket = constants.DefaultBucket
	}

	data, err := node.HandleStore(req.Key, req.Bucket, req.Value)

	if err != nil {
		// log.Printf("Store failed: %v", err)

		if errors.Is(err, ErrNotLeader) {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_NOT_LEADER,
				Value:   data,
			}, nil
		}

		if errors.Is(err, ErrTimeOut) {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_TIMEOUT,
			}, nil
		}

		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_UNEXPECTED,
		}, nil
	}

	return &rcppb.ClientResponse{
		Success: true,
	}, nil
}

func (node *Node) Get(ctx context.Context, req *rcppb.GetRequest) (*rcppb.ClientResponse, error) {
	if !node.Live {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_ALIVE,
		}, nil
	}

	if req.Key == "" {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
		}, nil
	}

	if req.Bucket == "" {
		req.Bucket = constants.DefaultBucket
	}

	data, err := node.HandleGet(req.Key, req.Bucket)

	if err != nil {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_FOUND,
		}, nil
	}

	return &rcppb.ClientResponse{
		Success: true,
		Value:   data,
	}, nil
}

func (node *Node) Delete(ctx context.Context, req *rcppb.DeleteRequest) (*rcppb.ClientResponse, error) {
	if !node.Live {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_NOT_ALIVE,
		}, nil
	}

	if req.Key == "" {
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
		}, nil
	}

	if req.Bucket == "" {
		req.Bucket = constants.DefaultBucket
	}

	data, err := node.HandleDelete(req.Key, req.Bucket)

	if err != nil {
		log.Printf("Delete failed: %v", err)

		if errors.Is(err, ErrNotLeader) {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_NOT_LEADER,
				Value:   data,
			}, nil
		}

		if errors.Is(err, ErrTimeOut) {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_TIMEOUT,
			}, nil
		}

		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_UNEXPECTED,
		}, nil
	}

	return &rcppb.ClientResponse{
		Success: true,
	}, nil
}

func (node *Node) CauseFailure(ctx context.Context, req *rcppb.CauseFailureRequest) (*rcppb.ClientResponse, error) {
	log.Printf("Got cause-failure of type %s", req.Type)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	switch req.Type {
	case rcppb.FailureType_REVIVE:
		if node.Live {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_UNEXPECTED,
				Value:   "still alive",
			}, nil
		} else {
			node.Live = true
			return &rcppb.ClientResponse{
				Success: true,
			}, nil
		}

	case rcppb.FailureType_LEADER:
		if node.Live {
			if node.isLeader {
				node.Live = false
				node.StepDownLocked()
				return &rcppb.ClientResponse{
					Success: true,
				}, nil
			} else {
				// Node is not the leader, redirect to the leader
				return &rcppb.ClientResponse{
					Success: false,
					Error:   rcppb.ErrorType_NOT_LEADER,
					Value:   node.votedFor,
				}, nil
			}
		} else {
			// Node is not alive, the node doesn't know the correct leader
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_NOT_ALIVE,
			}, nil
		}

	case rcppb.FailureType_REPLICA:
		if node.isLeader {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_UNEXPECTED,
				Value:   "is leader",
			}, nil
		} else {
			if node.Live {
				node.Live = false
				return &rcppb.ClientResponse{
					Success: true,
				}, nil
			} else {
				return &rcppb.ClientResponse{
					Success: false,
					Error:   rcppb.ErrorType_NOT_ALIVE,
				}, nil
			}
		}

	case rcppb.FailureType_RANDOM:
		if node.Live {
			node.Live = false
			node.StepDownLocked()
			return &rcppb.ClientResponse{
				Success: true,
			}, nil
		} else {
			return &rcppb.ClientResponse{
				Success: false,
				Error:   rcppb.ErrorType_NOT_ALIVE,
			}, nil
		}

	default:
		return &rcppb.ClientResponse{
			Success: false,
			Error:   rcppb.ErrorType_BAD_REQUEST,
		}, nil
	}
}

// func (node *Node) Delay(ctx context.Context, req *rcppb.DelayRequest) (*wrapperspb.BoolValue, error) {
// 	to := req.NodeId
// 	delay := req.Delay

// 	if delay <= 0 || to == "" {
// 		return nil, errors.New("invalid request argument")
// 	}

// 	log.Printf("Setting delay: To=%s, Delay=%d", to, delay)

// 	node.delays.Store(to, delay)
// 	return &wrapperspb.BoolValue{Value: true}, nil
// }

// func (node *Node) CauseFailure(ctx context.Context, req *rcppb.CauseFailureRequest) (*wrapperspb.BoolValue, error) {
// 	failureType := req.Type
// 	log.Printf("Got cause-failure of type %s", failureType)
// 	var nodeToKill string
// 	switch failureType {
// 	case "leader":
// 		// currentLeader, ok := node.votedFor.Load(node.currentTerm)
// 		// if !ok {
// 		// 	return nil, fmt.Errorf("BUG() no leader")
// 		// }
// 		// nodeToKill = currentLeader.(string)
// 		nodeToKill = node.votedFor
// 	case "non-leader":
// 		// currentLeader, ok := node.votedFor.Load(node.currentTerm)
// 		// if !ok {
// 		// 	return nil, fmt.Errorf("BUG() no leader")
// 		// }
// 		currentLeader := node.votedFor

// 		for nodeId := range node.ClientMap {
// 			if nodeId != currentLeader {
// 				if _, failed := node.failedSet[nodeId]; failed {
// 					nodeToKill = nodeId
// 					break
// 				}
// 			}
// 		}
// 	case "random":
// 		for nodeId := range node.ClientMap {
// 			if _, failed := node.failedSet[nodeId]; failed {
// 				nodeToKill = nodeId
// 				break
// 			}
// 		}
// 	default:
// 		return nil, fmt.Errorf("invalid failure type. should be leader/non-leader/random")
// 	}

// 	if nodeToKill == node.Id {
// 		_, err := node.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		RPCClient, ok := node.ClientMap[nodeToKill]
// 		if !ok {
// 			return nil, fmt.Errorf("invalid server or no gRPC client for '%s'", nodeToKill)
// 		}
// 		RPCClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
// 	}

// 	return &wrapperspb.BoolValue{Value: true}, nil
// }
