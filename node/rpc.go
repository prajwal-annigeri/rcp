package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/db"
	"rcp/rcppb"
	"sync"
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

	if appendEntryReq.Delay > 0 {
		log.Printf("Delay %s to %s: %d", appendEntryReq.LeaderId, node.Id, appendEntryReq.Delay)
	}
	delay := time.After(time.Duration(appendEntryReq.Delay) * time.Millisecond)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	<-delay

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
	// begin := time.Now()
	// time.Sleep(time.Duration(requestVoteReq.Delay) * time.Millisecond)
	// log.Printf("Slept for %v", time.Since(begin))
	
	// node.reachableSetLock.RLock()
	// _, reachable := node.reachableNodes[requestVoteReq.CandidateId]
	// node.reachableSetLock.RUnlock()

	// if !reachable {
	// 	log.Printf("Received RequestVote: %s Term: %d I'm not reachable", requestVoteReq.CandidateId, requestVoteReq.Term)
	// 	return &rcppb.RequestVoteResponse{
	// 		Term:        node.currentTerm,
	// 		VoteGranted: false,
	// 	}, status.Error(codes.Unavailable, "not reachable")
	// }
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
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		defer node.callbackChannelMap.Delete(callbackChannelId)
		begin := time.Now()
		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType:           "store",
				Key:               KV.Key,
				Value:             KV.Value,
				CallbackChannelId: callbackChannelId,
			}
		}()

		// callbackChannelRaw, ok := node.callbackChannelMap.Load(callbackChannelId)
		// if !ok {
		// 	log.Printf("BUG: no callback channel")
		// 	return nil, errors.New("no callback channel")
		// }

		// callbackChannel := callbackChannelRaw.(chan struct{})
		log.Printf("Time before select: %v", time.Since(begin))
		select {
		case <-callbackChannel:
			log.Println("Got callback")
			log.Printf("Time: %v", time.Since(begin))
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(20 * time.Second):
			log.Printf("TIMED OUT Store")
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

func (node *Node) GetBalance(ctx context.Context, req *rcppb.GetBalanceRequest) (*rcppb.GetBalanceResponse, error) {
	// err := node.db.Lock(context.Background(), req.AccountId)
	// defer node.db.Unlock(req.AccountId)
	// if err != nil {
	// 	return nil, err
	// }
	var checking, savings int64
	var err1, err2 error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		checking, err1 = node.db.GetBalance(req.AccountId, db.CheckingAccount)
	}()

	go func() {
		defer wg.Done()
		savings, err2 = node.db.GetBalance(req.AccountId, db.SavingsAccount)
	}()

	wg.Wait()

	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
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
			return grpcClient.DepositChecking(ctx, req)
		} else {
			return nil, fmt.Errorf("no grpc client for %s", leader)
		}
	} else {
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		begin := time.Now()
		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "bank",
				Transaction1: &rcppb.Transaction{
					AccountId:   req.AccountId,
					Amount:      req.Amount,
					AccountType: string(db.CheckingAccount),
				},
				CallbackChannelId: callbackChannelId,
			}
		}()

		select {
		case <-callbackChannel:
			log.Printf("Got callback: %v", time.Since(begin))

			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out")
		}
	}
	// return &wrapperspb.BoolValue{Value: true}, nil

}

func (node *Node) WriteCheck(ctx context.Context, req *rcppb.WriteCheckRequest) (*wrapperspb.BoolValue, error) {
	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req to leader: %s\n", leader.(string))
			return grpcClient.WriteCheck(ctx, req)
		} else {
			return nil, fmt.Errorf("no grpc client for %s", leader)
		}
	} else {
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		err := node.db.Lock(ctx, req.AccountId)
		defer node.db.Unlock(req.AccountId)
		if err != nil {
			return nil, err
		}
		balance, err := node.db.GetBalance(req.AccountId, db.CheckingAccount)
		if err != nil {
			return nil, err
		}

		if balance < req.Amount {
			return nil, fmt.Errorf("insufficient balance: %d", balance)
		}

		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "bank",
				Transaction1: &rcppb.Transaction{
					AccountId:   req.AccountId,
					Amount:      -1 * req.Amount,
					AccountType: string(db.CheckingAccount),
				},
				CallbackChannelId: callbackChannelId,
			}
		}()

		select {
		case <-callbackChannel:
			log.Println("Got callback")
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out")
		}
	}
	// return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) SendPayment(ctx context.Context, req *rcppb.SendPaymentRequest) (*wrapperspb.BoolValue, error) {
	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req to leader: %s\n", leader.(string))
			return grpcClient.SendPayment(ctx, req)
		} else {
			return nil, fmt.Errorf("no grpc client for %s", leader)
		}
	} else {
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		err := node.db.Lock(ctx, req.AccountIdFrom)
		defer node.db.Unlock(req.AccountIdFrom)
		if err != nil {
			return nil, err
		}
		err = node.db.Lock(ctx, req.AccountIdTo)
		defer node.db.Unlock(req.AccountIdTo)
		if err != nil {
			return nil, err
		}
		balance, err := node.db.GetBalance(req.AccountIdFrom, db.CheckingAccount)
		if err != nil {
			return nil, err
		}

		if balance < req.Amount {
			return nil, fmt.Errorf("insufficient balance: %d", balance)
		}

		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "bank",
				Transaction1: &rcppb.Transaction{
					AccountId:   req.AccountIdFrom,
					Amount:      -1 * req.Amount,
					AccountType: string(db.CheckingAccount),
				},
				Transaction2: &rcppb.Transaction{
					AccountId:   req.AccountIdTo,
					Amount:      req.Amount,
					AccountType: string(db.CheckingAccount),
				},
				CallbackChannelId: callbackChannelId,
			}
		}()

		select {
		case <-callbackChannel:
			log.Println("Got callback")
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out")
		}
	}
}

func (node *Node) TransactSavings(ctx context.Context, req *rcppb.TransactSavingsRequest) (*wrapperspb.BoolValue, error) {
	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req to leader: %s\n", leader.(string))
			return grpcClient.TransactSavings(ctx, req)
		} else {
			return nil, fmt.Errorf("no grpc client for %s", leader)
		}
	} else {
		now := time.Now()
		err := node.db.Lock(ctx, req.AccountId)
		defer node.db.Unlock(req.AccountId)
		if err != nil {
			return nil, err
		}

		balance, err := node.db.GetBalance(req.AccountId, db.SavingsAccount)
		if err != nil {
			return nil, err
		}

		newBalance := balance + req.Amount
		if newBalance < 0 {
			return nil, fmt.Errorf("insufficient balance: %d", balance)
		}
		log.Printf("Time before creating callback channel: %v", time.Since(now))
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "bank",
				Transaction1: &rcppb.Transaction{
					AccountId:   req.AccountId,
					Amount:      req.Amount,
					AccountType: string(db.SavingsAccount),
				},
				CallbackChannelId: callbackChannelId,
			}
		}()

		select {
		case <-callbackChannel:
			log.Printf("Got callback: %v", time.Since(now))
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out")
		}
	}
}

func (node *Node) Amalgamate(ctx context.Context, req *rcppb.AmalgamateRequest) (*wrapperspb.BoolValue, error) {
	if !node.isLeader {
		leader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			log.Println("BUG: NO LEADER")
			return nil, errors.New("no leader")
		}
		grpcClient, ok := node.ClientMap[leader.(string)]
		if ok {
			log.Printf("Forwarded req to leader: %s\n", leader.(string))
			return grpcClient.Amalgamate(ctx, req)
		} else {
			return nil, fmt.Errorf("no grpc client for %s", leader)
		}
	} else {
		callbackChannelId, callbackChannel := node.makeCallbackChannel()
		err := node.db.Lock(ctx, req.AccountIdFrom)
		defer node.db.Unlock(req.AccountIdFrom)
		if err != nil {
			return nil, err
		}
		err = node.db.Lock(ctx, req.AccountIdTo)
		defer node.db.Unlock(req.AccountIdTo)
		if err != nil {
			return nil, err
		}
		balance, err := node.db.GetBalance(req.AccountIdFrom, db.SavingsAccount)
		if err != nil {
			return nil, err
		}

		go func() {
			node.logBufferChan <- &rcppb.LogEntry{
				LogType: "bank",
				Transaction1: &rcppb.Transaction{
					AccountId:   req.AccountIdFrom,
					Amount:      -1 * balance,
					AccountType: string(db.SavingsAccount),
				},
				Transaction2: &rcppb.Transaction{
					AccountId:   req.AccountIdTo,
					Amount:      balance,
					AccountType: string(db.CheckingAccount),
				},
				CallbackChannelId: callbackChannelId,
			}
		}()

		select {
		case <-callbackChannel:
			log.Println("Got callback")
			return &wrapperspb.BoolValue{Value: true}, nil
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out")
		}
	}
}
