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
		log.Printf("Recieved AppendEntries from %s with %d entries. Term: %d\n", appendEntryReq.LeaderId, len(appendEntryReq.Entries), appendEntryReq.Term)
	}
	
	if !node.Live {
		return &rcppb.AppendEntriesResponse{
			Term: node.currentTerm,
			Success: false,
		}, errors.New("not alive")
	}


	if appendEntryReq.Term < node.currentTerm {
		log.Printf("Denying append because my term %d is > %d\n", node.currentTerm, appendEntryReq.Term)
		return &rcppb.AppendEntriesResponse{
			Term: node.currentTerm,
			Success: false,
		}, fmt.Errorf("%s denied append because its term is %d which is greater than %d", node.Id, node.currentTerm, appendEntryReq.Term)
	}

	if appendEntryReq.PrevLogIndex >= 0 {
		// prevLogEntry, err := node.db.GetLogAtIndex(appendEntryReq.PrevLogIndex)
		// if err != nil {
		// 	return &rcppb.AppendEntriesResponse{
		// 		Term: -1,
		// 		Success: false,
		// 	}, err
		// }
	
		if node.lastTerm != appendEntryReq.PrevLogTerm {
			return &rcppb.AppendEntriesResponse{
				Term: node.lastTerm,
				Success: false,
			}, fmt.Errorf("prev log entry term does not match, mine: %d, in req: %d", node.lastTerm, appendEntryReq.Term)
		}
	}

	if node.currentTerm < appendEntryReq.Term {
		node.isLeader = false
		node.currentTerm = appendEntryReq.Term
	}
	// log.Printf("Received heartbeat from %s, resetting election timer", appendEntryReq.LeaderId)
	// node.electionTimer.Reset(randomElectionTimeout())
	node.resetElectionTimer()
	node.currentTerm = appendEntryReq.Term
	

	err := node.insertLogs(appendEntryReq)
	if err != nil {
		return &rcppb.AppendEntriesResponse{
			Term: node.currentTerm,
			Success: false,
		}, err
	}

	node.commitIndex = appendEntryReq.LeaderCommit

	return &rcppb.AppendEntriesResponse{
		Term: node.currentTerm,
		Success: true,
	}, nil
}

func (node *Node) insertLogs(appendEntryReq *rcppb.AppendEntriesReq) error {
	if len(appendEntryReq.Entries) > 0 {
		log.Printf("Inserting logs: %v\n", appendEntryReq.Entries)
	}
	err := node.db.DeleteLogsStartingFromIndex(appendEntryReq.PrevLogIndex + 1)
	if err != nil {
		return err
	}
	err = node.db.InsertLogs(appendEntryReq)
	if err != nil {
		return err
	}
	if len(appendEntryReq.Entries) > 0 {
		node.lastIndex = appendEntryReq.PrevLogIndex + int64(len(appendEntryReq.Entries))
		node.lastTerm = appendEntryReq.Term
	}
	return nil
}
		

func (node *Node) RequestVote(ctx context.Context, requestVoteReq *rcppb.RequestVoteReq) (*rcppb.RequestVoteResponse, error) {
	if !node.Live {
		log.Printf("I'm not alive")
		return &rcppb.RequestVoteResponse{
			Term: node.currentTerm,
			VoteGranted: false,
		}, nil
	}
	log.Printf("Received RequestVote from %s: Term %d\n", requestVoteReq.CandidateId, requestVoteReq.Term)
	if requestVoteReq.Term < node.currentTerm {
		log.Printf("Denying vote to %s as my term is greater\n", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term: node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if requestVoteReq.LastLogTerm < node.lastTerm || (requestVoteReq.LastLogTerm == node.lastTerm && node.lastIndex > requestVoteReq.LastLogIndex) {
		log.Printf("Denying vote to %s as I have a more complete log\n", requestVoteReq.CandidateId)
		return &rcppb.RequestVoteResponse{
			Term: node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// if (node.votedFor == requestVoteReq.CandidateId || node.votedFor == "") {
	// 	log.Printf("Voting for %s\n", requestVoteReq.CandidateId)
	// 	return &rcppb.RequestVoteResponse{
	// 		Term: node.currentTerm,
	// 		VoteGranted: true,
	// 	}, nil
	// }

	
	log.Printf("Voting for %s\n", requestVoteReq.CandidateId)
	node.resetElectionTimer()
	node.votedFor = requestVoteReq.CandidateId
	node.currentTerm = requestVoteReq.Term
	return &rcppb.RequestVoteResponse{
		Term: node.currentTerm,
		VoteGranted: true,
	}, nil
}
