package node

import (
	"context"
	"fmt"
	"log"
	"rcp/rcppb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	node.reachableSetLock.RLock()
	defer node.reachableSetLock.RUnlock()
	_, reachable := node.reachableNodes[appendEntryReq.LeaderId]
	if !reachable {
		log.Printf("Received AppendEntries: %s Term: %d I'm not reachable", appendEntryReq.LeaderId, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term:        node.currentTerm,
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
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Inserting logs: %v\n", appendEntryReq.Entries)
	}
	currIndex := appendEntryReq.PrevLogIndex + 1
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
		node.lastTerm = appendEntryReq.Term
		currIndex += 1
	}
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
	node.reachableSetLock.RLock()
	defer node.reachableSetLock.RUnlock()
	_, reachable := node.reachableNodes[requestVoteReq.CandidateId]
	if !reachable {
		log.Printf("Received RequestVote: %s Term: %d I'm not reachable", requestVoteReq.CandidateId, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.Unavailable, "not reachable")
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

	votedFor, ok := node.votedFor.Load(requestVoteReq.Term)
	if !ok || votedFor.(string) == requestVoteReq.CandidateId {
		log.Printf("Voting for %s\n", requestVoteReq.CandidateId)
		node.resetElectionTimer()
		node.votedFor.Store(requestVoteReq.Term, requestVoteReq.CandidateId)
		node.currentTerm = max(node.currentTerm, requestVoteReq.Term)
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: true,
		}, nil
	}
	return &rcppb.RequestVoteResponse{
		Term:        node.currentTerm,
		VoteGranted: false,
	}, status.Error(codes.PermissionDenied, fmt.Sprintf("already voted for %s for term %d", votedFor.(string), requestVoteReq.Term))
}

func (node *Node) Store(ctx context.Context, KV *rcppb.KV) (*rcppb.StoreKVResponse, error) {
	if node.isLeader {
		node.logBufferChan <- &rcppb.LogEntry{
			LogType: "store",
			Key:     KV.Key,
			Value:   KV.Value,
		}
	}
	return &rcppb.StoreKVResponse{
		Success: true,
	}, nil
}
