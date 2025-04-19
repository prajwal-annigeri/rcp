package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/db"
	"rcp/rcppb"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (node *Node) AppendEntries(ctx context.Context, appendEntryReq *rcppb.AppendEntriesReq) (*rcppb.AppendEntriesResponse, error) {
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Received AppendEntries from %s with %d entries. Term: %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term)
	}

	if !node.Live {
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Unavailable, "not alive")
	}

	// begin := time.Now()
	// time.Sleep(time.Duration(appendEntryReq.Delay) * time.Millisecond)
	// log.Printf("Slept for %v", time.Since(begin))

	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.reachableSetLock.RLock()
	_, reachable := node.reachableNodes[appendEntryReq.LeaderId]
	node.reachableSetLock.RUnlock()
	if !reachable {
		log.Printf("Received AppendEntries: %s Term: %d I'm not reachable", appendEntryReq.LeaderId, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, status.Error(codes.Unavailable, "not reachable")
	}

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

	return &rcppb.AppendEntriesResponse{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}

func (node *Node) insertLogs(appendEntryReq *rcppb.AppendEntriesReq) error {
	if len(appendEntryReq.Entries) == 0 {
		return nil
	}

	log.Printf("Inserting logs: %v\n", appendEntryReq.Entries)

	currIndex := appendEntryReq.PrevLogIndex + 1
	lastEntryTerm := appendEntryReq.Term
	// var err error
	for _, entry := range appendEntryReq.Entries {
		log.Printf("Putting entry: %v\n", entry)
		failedNode, recoveredNode, err := node.db.PutLogAtIndex(currIndex, entry)

		if err != nil {
			return err
		}
		if failedNode != "" {
			node.removeFromFailureSet(failedNode)
		} else if recoveredNode != "" {
			node.removeFromRecoverySet(recoveredNode)
		}
		node.lastIndex = currIndex
		lastEntryTerm = entry.Term
		currIndex += 1
	}

	node.lastTerm = lastEntryTerm
	// return nil
	// err := node.db.InsertLogs(appendEntryReq)
	// if err != nil {
	// 	return err
	// }
	// node.lastIndex = appendEntryReq.PrevLogIndex + int64(len(appendEntryReq.Entries))
	// node.lastTerm = appendEntryReq.Term
	return nil
}

// func (node *Node) insertLogAtIndex(index int64, entry *rcppb.LogEntry) error {
// 	existingLog, err := node.db.GetLogAtIndex(index)
// 	if err == nil {
// 		if existingLog.LogType == "failure" {
// 			node.removeFromFailureSet()
// 		} else if existingLog.LogType == "recovery" {
// 			node.removeFromRecoveryWaitSet()
// 		}
// 	}
// 	err = node.db.PutLogAtIndex(index, entry)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (node *Node) RequestVote(ctx context.Context, requestVoteReq *rcppb.RequestVoteReq) (*rcppb.RequestVoteResponse, error) {
	if !node.Live {
		log.Printf("Received RequestVote: %s Term: %d I'm not alive", requestVoteReq.CandidateId, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.Unavailable, "not alive")
	}
	// begin := time.Now()
	// time.Sleep(time.Duration(requestVoteReq.Delay) * time.Millisecond)
	// log.Printf("Slept for %v", time.Since(begin))
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.reachableSetLock.RLock()
	_, reachable := node.reachableNodes[requestVoteReq.CandidateId]
	node.reachableSetLock.RUnlock()

	if !reachable {
		log.Printf("Received RequestVote: %s Term: %d I'm not reachable", requestVoteReq.CandidateId, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.Unavailable, "not reachable")
	}
	log.Printf("Received RequestVote from %s: Term %d", requestVoteReq.CandidateId, requestVoteReq.Term)

	// // If candidate's term is higher, update currentTerm and clear votedFor
	// if requestVoteReq.Term > node.currentTerm {
	// 	node.currentTerm = requestVoteReq.Term
	// 	node.votedFor.Store(requestVoteReq.Term, "")
	// }

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

func (node *Node) Store(ctx context.Context, KV *rcppb.KV) (*wrapperspb.BoolValue, error) {

	if KV.Key == "" {
		return nil, errors.New("key cannot be empty")
	}

	log.Printf("Received data: Key=%s, Value=%s", KV.Key, KV.Value)

	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req with key: %s, value: %s to leader: %s\n", KV.Key, KV.Value, leader.(string))
			return grpcClient.Store(context.Background(), &rcppb.KV{Key: KV.Key, Value: KV.Value})
		}
	} else {
		callbackChannelId := node.makeCallbackChannel()
		node.logBufferChan <- &rcppb.LogEntry{
			LogType:           "store",
			Key:               KV.Key,
			Value:             KV.Value,
			CallbackChannelId: callbackChannelId,
		}

		callbackChannelRaw, ok := node.callbackChannelMap.Load(callbackChannelId)
		if !ok {
			log.Printf("BUG: no callback channel")
			return nil, errors.New("no callback channel")
		}

		callbackChannel := callbackChannelRaw.(chan struct{})

		select {
		case <-callbackChannel:
			log.Println("Got callback")
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(1 * time.Second):
			return nil, errors.New("timed out")
		}
	}
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) Get(ctx context.Context, req *rcppb.GetValueReq) (*rcppb.GetValueResponse, error) {

	value, err := node.db.GetKV(req.Key)

	if err != nil {
		log.Printf("Error getting value for %s: %v\n", req.Key, err)
		return nil, err
	}
	return &rcppb.GetValueResponse{Success: true, Value: value}, nil
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


func (node *Node) GetBalance(ctx context.Context, req *rcppb.GetBalanceRequest) (*rcppb.GetBalanceResponse, error){
	err := node.db.Lock(context.Background(), req.AccountId)
	defer node.db.Unlock(req.AccountId)
	if err != nil {
		return nil, err
	}
	checking, err := node.db.GetBalance(req.AccountId, db.CheckingAccount)
	if err != nil {
		return nil, err
	}
	savings, err := node.db.GetBalance(req.AccountId, db.SavingsAccount)
	if err != nil {
		return nil, err
	}

	return &rcppb.GetBalanceResponse{CheckingBalance: checking, SavingsBalance: savings}, nil
}

func (node *Node) DepositChecking(ctx context.Context, req *rcppb.DepositCheckingRequest) (*wrapperspb.BoolValue, error) {
	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req to leader: %s\n", leader.(string))
			return grpcClient.DepositChecking(context.Background(), req)
		}
	} else {
		callbackChannelId := node.makeCallbackChannel()
		node.logBufferChan <- &rcppb.LogEntry{
			LogType: "bank",
			Transaction1: &rcppb.Transaction{
				AccountId: req.AccountId,
				Amount: req.Amount,
				AccountType: string(db.CheckingAccount),
			},
			CallbackChannelId: callbackChannelId,
		}

		callbackChannelRaw, ok := node.callbackChannelMap.Load(callbackChannelId)
		if !ok {
			log.Printf("BUG: no callback channel")
			return nil, errors.New("no callback channel")
		}

		callbackChannel := callbackChannelRaw.(chan struct{})

		select {
		case <-callbackChannel:
			log.Println("Got callback")
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(1 * time.Second):
			return nil, errors.New("timed out")
		}
	}
	return &wrapperspb.BoolValue{Value: true}, nil
	
}
