package node

import (
	"context"
	"fmt"
	"log"
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
		log.Printf("LOGX Received AppendEntries from %s with %d entries. Term: %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term)
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
		node.StepDown()
	}

	// TODO: Handle if log removal needed
	if appendEntryReq.PrevLogIndex >= 0 {
		prevLogTerm := int64(-1)
		logEntry, err := node.db.GetLogAtIndex(appendEntryReq.PrevLogIndex)

		if err == nil {
			prevLogTerm = logEntry.Term
		}

		if prevLogTerm != appendEntryReq.PrevLogTerm {
			log.Printf("Denying append entry because prev log entryterm does not match, mine: %d, in req: %d", prevLogTerm, appendEntryReq.PrevLogTerm)
			return &rcppb.AppendEntriesResponse{
				Term:    node.currentTerm,
				Success: false,
			}, nil
		}
	}

	// log.Println("Reset election timer")
	node.resetElectionTimer()

	// Append any new entries
	err := node.insertLogs(appendEntryReq)
	if err != nil {
		log.Panic("Error inserting logs")
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Internal, err.Error())
	}

	// Update commit index
	if appendEntryReq.LeaderCommit > node.commitIndex {
		node.commitIndex = min(appendEntryReq.LeaderCommit, node.GetLastIndex())
		err = node.executeUntil(node.commitIndex)
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
func (node *Node) insertLogs(appendEntryReq *rcppb.AppendEntriesReq) error {
	if len(appendEntryReq.Entries) == 0 {
		return nil
	}

	currIndex := appendEntryReq.PrevLogIndex + 1
	// lastEntryTerm := appendEntryReq.Term

	for _, entry := range appendEntryReq.Entries {
		node.InsertLog(entry, currIndex)

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
		node.StepDown()
		node.votedFor = ""
		node.resetElectionTimer()
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
	if requestVoteReq.LastLogTerm < node.GetLastTerm() || (requestVoteReq.LastLogTerm == node.GetLastTerm() && node.GetLastIndex() > requestVoteReq.LastLogIndex) {
		log.Printf("Denying vote to %s as I have a more complete log", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	log.Printf("Voting for %s for term %d\n", requestVoteReq.CandidateId, requestVoteReq.Term)
	node.currentTerm = requestVoteReq.Term
	node.votedFor = requestVoteReq.CandidateId
	node.StepDown()

	return &rcppb.RequestVoteResponse{
		Term:        node.currentTerm,
		VoteGranted: true,
	}, nil
}

func (node *Node) SetStatus(ctx context.Context, req *wrapperspb.BoolValue) (*wrapperspb.BoolValue, error) {
	log.Printf("Setting status: %t", req.Value)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	if req.Value {
		node.StepDown()
	}
	node.Live = req.Value

	return &wrapperspb.BoolValue{Value: true}, nil
}

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

func (node *Node) CauseFailure(ctx context.Context, req *rcppb.CauseFailureRequest) (*wrapperspb.BoolValue, error) {
	failureType := req.Type
	log.Printf("Got cause-failure of type %s", failureType)
	var nodeToKill string
	switch failureType {
	case "leader":
		// currentLeader, ok := node.votedFor.Load(node.currentTerm)
		// if !ok {
		// 	return nil, fmt.Errorf("BUG() no leader")
		// }
		// nodeToKill = currentLeader.(string)
		nodeToKill = node.votedFor
	case "non-leader":
		// currentLeader, ok := node.votedFor.Load(node.currentTerm)
		// if !ok {
		// 	return nil, fmt.Errorf("BUG() no leader")
		// }
		currentLeader := node.votedFor

		for nodeId := range node.ClientMap {
			if nodeId != currentLeader {
				if _, failed := node.failedSet[nodeId]; failed {
					nodeToKill = nodeId
					break
				}
			}
		}
	case "random":
		for nodeId := range node.ClientMap {
			if _, failed := node.failedSet[nodeId]; failed {
				nodeToKill = nodeId
				break
			}
		}
	default:
		return nil, fmt.Errorf("invalid failure type. should be leader/non-leader/random")
	}

	if nodeToKill == node.Id {
		_, err := node.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
		if err != nil {
			return nil, err
		}
	} else {
		RPCClient, ok := node.ClientMap[nodeToKill]
		if !ok {
			return nil, fmt.Errorf("invalid server or no gRPC client for '%s'", nodeToKill)
		}
		RPCClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
	}

	return &wrapperspb.BoolValue{Value: true}, nil
}
