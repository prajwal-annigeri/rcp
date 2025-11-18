package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"rcp/constants"
	db "rcp/db"
	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (node *Node) AppendEntries(ctx context.Context, appendEntryReq *orcapb.AppendEntriesRequest) (*orcapb.AppendEntriesResponse, error) {
	if !node.Live {
		return &orcapb.AppendEntriesResponse{
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
		return &orcapb.AppendEntriesResponse{
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
			return &orcapb.AppendEntriesResponse{
				Term:    node.currentTerm,
				Success: false,
			}, nil
		}
	}

	// Append any new entries
	err := node.insertLogsLocked(appendEntryReq)
	if err != nil {
		log.Panic("Error inserting logs")
		return &orcapb.AppendEntriesResponse{
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

	return &orcapb.AppendEntriesResponse{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}

// This function assume mutex is already locked
func (node *Node) insertLogsLocked(appendEntryReq *orcapb.AppendEntriesRequest) error {
	if len(appendEntryReq.Entries) == 0 {
		return nil
	}

	currIndex := appendEntryReq.PrevLogIndex + 1

	for _, entry := range appendEntryReq.Entries {
		node.InsertLogLocked(entry, currIndex)
		currIndex++
	}

	return nil
}

func (node *Node) RequestVote(ctx context.Context, requestVoteReq *orcapb.RequestVoteRequest) (*orcapb.RequestVoteResponse, error) {
	if !node.Live {
		log.Printf("Received RequestVote: %s Term: %d I'm not alive", requestVoteReq.CandidateId, requestVoteReq.Term)
		return &orcapb.RequestVoteResponse{
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
		return &orcapb.RequestVoteResponse{
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
		return &orcapb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, status.Error(codes.PermissionDenied, fmt.Sprintf("already voted for %s for term %d", node.votedFor, requestVoteReq.Term))
	}

	// Check if candidate is up to date
	if requestVoteReq.LastLogTerm < node.GetLastTermLocked() || (requestVoteReq.LastLogTerm == node.GetLastTermLocked() && node.GetLastIndexLocked() > requestVoteReq.LastLogIndex) {
		log.Printf("Denying vote to %s as I have a more complete log", requestVoteReq.CandidateId)
		return &orcapb.RequestVoteResponse{
			Term:        node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	log.Printf("Voting for %s for term %d\n", requestVoteReq.CandidateId, requestVoteReq.Term)
	node.currentTerm = requestVoteReq.Term
	node.votedFor = requestVoteReq.CandidateId
	node.StepDownLocked()

	return &orcapb.RequestVoteResponse{
		Term:        node.currentTerm,
		VoteGranted: true,
	}, nil
}

func (node *Node) Health(ctx context.Context, req *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return &wrapperspb.BoolValue{Value: true}, nil
}

func (node *Node) PerformOperation(ctx context.Context, req *kvpb.KVRequest) (*kvpb.ClientResponse, error) {
	if !node.Live {
		return &kvpb.ClientResponse{
			Success: false,
			Error:   kvpb.ErrorType_NOT_ALIVE,
		}, nil
	}

	if req.Key == "" {
		return &kvpb.ClientResponse{
			Success: false,
			Error:   kvpb.ErrorType_BAD_REQUEST,
			Value:   "missing key",
		}, nil
	}

	bucket := req.Bucket
	if bucket == "" {
		bucket = constants.DefaultBucket
	}

	switch req.Op {
	case kvpb.OperationType_STORE:
		if req.Value == "" {
			return &kvpb.ClientResponse{
				Success: false,
				Error:   kvpb.ErrorType_BAD_REQUEST,
				Value:   "missing value",
			}, nil
		}
		_, err := node.HandleStore(req.Key, bucket, req.Value)
		if resp, kvErr := node.handleKVError(err); kvErr != nil || resp != nil {
			return resp, kvErr
		}
		return &kvpb.ClientResponse{Success: true}, nil
	case kvpb.OperationType_GET:
		value, err := node.HandleGet(req.Key, bucket)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return &kvpb.ClientResponse{
					Success: false,
					Error:   kvpb.ErrorType_NOT_FOUND,
				}, nil
			}
			return &kvpb.ClientResponse{
				Success: false,
				Error:   kvpb.ErrorType_BAD_REQUEST,
			}, nil
		}
		return &kvpb.ClientResponse{
			Success: true,
			Value:   value,
		}, nil
	case kvpb.OperationType_DELETE:
		_, err := node.HandleDelete(req.Key, bucket)
		if resp, kvErr := node.handleKVError(err); kvErr != nil || resp != nil {
			return resp, kvErr
		}
		return &kvpb.ClientResponse{Success: true}, nil
	default:
		return &kvpb.ClientResponse{
			Success: false,
			Error:   kvpb.ErrorType_BAD_REQUEST,
		}, nil
	}
}

func (node *Node) handleKVError(err error) (*kvpb.ClientResponse, error) {
	if err == nil {
		return nil, nil
	}
	if errors.Is(err, ErrNotLeader) {
		return &kvpb.ClientResponse{
			Success: false,
			Error:   kvpb.ErrorType_NOT_LEADER,
			Value:   node.votedFor,
		}, nil
	}
	if errors.Is(err, ErrTimeOut) {
		return &kvpb.ClientResponse{
			Success: false,
			Error:   kvpb.ErrorType_TIMEOUT,
		}, nil
	}
	return &kvpb.ClientResponse{
		Success: false,
		Error:   kvpb.ErrorType_UNEXPECTED,
	}, nil
}

func (node *Node) CauseFailure(ctx context.Context, req *orcapb.CauseFailureRequest) (*wrapperspb.BoolValue, error) {
	log.Printf("Got cause-failure of type %s", req.Type)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	switch req.Type {
	case orcapb.FailureType_REVIVE:
		if node.Live {
			return nil, status.Error(codes.FailedPrecondition, "still alive")
		}
		node.Live = true
		return &wrapperspb.BoolValue{Value: true}, nil

	case orcapb.FailureType_LEADER:
		if !node.Live {
			return nil, status.Error(codes.Unavailable, "not alive")
		}
		if !node.isLeader {
			return nil, status.Error(codes.PermissionDenied, node.votedFor)
		}
		node.Live = false
		node.StepDownLocked()
		return &wrapperspb.BoolValue{Value: true}, nil

	case orcapb.FailureType_REPLICA:
		if node.isLeader {
			return nil, status.Error(codes.FailedPrecondition, "is leader")
		}
		if !node.Live {
			return nil, status.Error(codes.Unavailable, "not alive")
		}
		node.Live = false
		return &wrapperspb.BoolValue{Value: true}, nil

	case orcapb.FailureType_RANDOM:
		if !node.Live {
			return nil, status.Error(codes.Unavailable, "not alive")
		}
		node.Live = false
		node.StepDownLocked()
		return &wrapperspb.BoolValue{Value: true}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown failure type")
	}
}
