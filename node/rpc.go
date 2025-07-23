package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/rcppb"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (node *Node) AppendEntries(ctx context.Context, appendEntryReq *rcppb.AppendEntriesReq) (*rcppb.AppendEntriesResponse, error) {
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("LOGX Received AppendEntries from %s with %d entries. Term: %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term)
	}

	if !node.Live {
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Unavailable, "not alive")
	}

	begin := time.Now()

	if appendEntryReq.Term < node.currentTerm {
		log.Printf("Denying append because my term %d is > %d\n", node.currentTerm, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Aborted, fmt.Sprintf("%s denied append because its term is %d which is greater than %d", node.Id, node.currentTerm, appendEntryReq.Term))
	}

	if node.currentTerm < appendEntryReq.Term {
		node.isLeader = false
		node.currentTerm = appendEntryReq.Term
	}

	delay := time.After(time.Duration(appendEntryReq.Delay) * time.Millisecond)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	<-delay

	if appendEntryReq.PrevLogIndex >= 0 {
		prevLogTerm := int64(-1)
		var logEntry *rcppb.LogEntry
		var err error
		err = nil
		if node.isPersistent {
			logEntry, err = node.db.GetLogAtIndex(appendEntryReq.PrevLogIndex)
		} else {
			logEntry, err = node.GetInMemoryLog(appendEntryReq.PrevLogIndex)
		}

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

	node.resetElectionTimer()

	err := node.insertLogs(appendEntryReq)
	if err != nil {
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Internal, err.Error())
	}

	if appendEntryReq.LeaderCommit > node.commitIndex {
		node.commitIndex = min(appendEntryReq.LeaderCommit, node.lastIndex)
	}
	end := time.Since(begin)
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Time for appendEntries with %d entries: %v", len(appendEntryReq.Entries), end)
	}

	return &rcppb.AppendEntriesResponse{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}

func (node *Node) insertLogs(appendEntryReq *rcppb.AppendEntriesReq) error {
	if len(appendEntryReq.Entries) == 0 {
		return nil
	}

	if node.isPersistent {
		return node.insertLogsPersistent(appendEntryReq)
	} else {
		return node.insertLogsInMemory(appendEntryReq)
	}

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

	if requestVoteReq.Term < node.currentTerm {
		log.Printf("Denying vote to %s as my term is greater", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if requestVoteReq.LastLogTerm < node.lastTerm || (requestVoteReq.LastLogTerm == node.lastTerm && node.lastIndex > requestVoteReq.LastLogIndex) {
		log.Printf("Denying vote to %s as I have a more complete log", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	node.mutex.Lock()
	defer node.mutex.Unlock()

	votedFor, ok := node.votedFor.Load(requestVoteReq.Term)
	if !ok || votedFor.(string) == requestVoteReq.CandidateId || votedFor.(string) == "" {
		log.Printf("Voting for %s\n", requestVoteReq.CandidateId)
		node.resetElectionTimer()
		node.votedFor.Store(requestVoteReq.Term, requestVoteReq.CandidateId)
		// node.currentTerm = max(node.currentTerm, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: true,
		}, nil
	}

	log.Printf("Already voted for %s in term %d", votedFor.(string), requestVoteReq.Term)
	return &rcppb.RequestVoteResponse{
		Term:        node.currentTerm,
		VoteGranted: false,
	}, status.Error(codes.PermissionDenied, fmt.Sprintf("already voted for %s for term %d", votedFor.(string), requestVoteReq.Term))
}

func (node *Node) SetStatus(ctx context.Context, req *wrapperspb.BoolValue) (*wrapperspb.BoolValue, error) {
	log.Printf("Setting status: %t", req.Value)
	if req.Value {
		node.resetElectionTimer()
	}
	node.Live = req.Value
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) Partition(ctx context.Context, req *rcppb.PartitionReq) (*wrapperspb.BoolValue, error) {
	node.reachableSetLock.Lock()
	defer node.reachableSetLock.Unlock()
	node.reachableNodes = make(map[string]struct{})
	for _, nodeId := range req.ReachableNodes {
		node.reachableNodes[nodeId] = struct{}{}
	}

	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) Delay(ctx context.Context, req *rcppb.DelayRequest) (*wrapperspb.BoolValue, error) {
	to := req.NodeId
	delay := req.Delay

	if delay <= 0 || to == "" {
		return nil, errors.New("invalid request argument")
	}

	log.Printf("Setting delay: To=%s, Delay=%d", to, delay)

	node.delays.Store(to, delay)
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) Healthz(ctx context.Context, req *rcppb.HealthzRequest) (*wrapperspb.BoolValue, error) {
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) CauseFailure(ctx context.Context, req *rcppb.CauseFailureRequest) (*wrapperspb.BoolValue, error) {
	failureType := req.Type
	log.Printf("Got cause-failure of type %s", failureType)
	var nodeToKill string
	switch failureType {
	case "leader":
		currentLeader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			return nil, fmt.Errorf("BUG() no leader")
		}
		nodeToKill = currentLeader.(string)
	case "non-leader":
		currentLeader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			return nil, fmt.Errorf("BUG() no leader")
		}

		for nodeID := range node.ClientMap {
			if nodeID != currentLeader {
				status, _ := node.serverStatusMap.Load(nodeID)
				if status.(bool) {
					nodeToKill = nodeID
					break
				}
			}
		}
	case "random":
		for nodeID := range node.ClientMap {
			status, _ := node.serverStatusMap.Load(nodeID)
			if status.(bool) {
				nodeToKill = nodeID
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
