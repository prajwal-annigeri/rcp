package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/rcppb"
)

func (node *Node) AppendEntries(ctx context.Context, appendEntryReq *rcppb.AppendEntriesReq) (*rcppb.AppendEntriesResponse, error) {
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Received AppendEntries from %s with %d entries. Term: %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term)
	}

	if !node.Live {
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, errors.New("not alive")
	}

	if appendEntryReq.Term < node.currentTerm {
		log.Printf("Denying append because my term %d is > %d\n", node.currentTerm, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term:    node.currentTerm,
			Success: false,
		}, fmt.Errorf("%s denied append because its term is %d which is greater than %d", node.Id, node.currentTerm, appendEntryReq.Term)
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
		}, err
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
	// err := node.db.DeleteLogsStartingFromIndex(appendEntryReq.PrevLogIndex + 1)
	// if err != nil {
	// 	return err
	// }
	err := node.db.InsertLogs(appendEntryReq)
	if err != nil {
		return err
	}
	// if len(appendEntryReq.Entries) > 0 {
	node.lastIndex = appendEntryReq.PrevLogIndex + int64(len(appendEntryReq.Entries))
	node.lastTerm = appendEntryReq.Term
	// }
	return nil
}

func (node *Node) RequestVote(ctx context.Context, requestVoteReq *rcppb.RequestVoteReq) (*rcppb.RequestVoteResponse, error) {
	if !node.Live {
		log.Printf("I'm not alive")
		return &rcppb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
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
	}, fmt.Errorf("already voted for %s for term %d", votedFor.(string), requestVoteReq.Term)
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
