package node

import (
	"rcp/db"
	"rcp/rcppb"
	"testing"
)

func makeSet(ids ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func TestHasElectionQuorumStable(t *testing.T) {
	n := &Node{
		reconfigMode:         ReconfigModeJoint,
		reconfigCurrentPhase: reconfigPhaseStable,
		activeVoterSet:       makeSet("A", "B", "C"),
		pendingVoterSet:      makeSet(),
	}

	if n.hasElectionQuorumLocked(makeSet("A")) {
		t.Fatalf("expected single vote to be insufficient for stable majority")
	}

	if !n.hasElectionQuorumLocked(makeSet("A", "B")) {
		t.Fatalf("expected two votes to satisfy stable majority")
	}
}

func TestHasElectionQuorumJointTransition(t *testing.T) {
	n := &Node{
		reconfigMode:         ReconfigModeJoint,
		reconfigCurrentPhase: reconfigPhaseTransition,
		activeVoterSet:       makeSet("A", "B", "C"),
		pendingVoterSet:      makeSet("B", "C", "D"),
	}

	if n.hasElectionQuorumLocked(makeSet("A", "B")) {
		t.Fatalf("expected votes to fail because new set majority is not met")
	}

	if !n.hasElectionQuorumLocked(makeSet("B", "C")) {
		t.Fatalf("expected overlapping votes to satisfy both old and new majorities")
	}
}

func TestHasCommitQuorumForIndexJointTransition(t *testing.T) {
	n := &Node{
		Id:                   "A",
		reconfigMode:         ReconfigModeJoint,
		reconfigCurrentPhase: reconfigPhaseTransition,
		activeVoterSet:       makeSet("A", "B", "C"),
		pendingVoterSet:      makeSet("B", "C", "D"),
		matchIndex: map[string]int64{
			"B": 5,
			"C": 2,
			"D": 5,
		},
	}

	n.db = db.InitMemoryDatabase()
	for i := 0; i <= 5; i++ {
		_, err := n.db.AppendLog(&rcppb.LogEntry{LogType: rcppb.LogType_STORE, Term: 1})
		if err != nil {
			t.Fatalf("failed to append test log %d: %v", i, err)
		}
	}

	// Old: A,B,C requires 2 of 3 replicated at index 5.
	// New: B,C,D requires 2 of 3 replicated at index 5.
	// Replicated at 5: A(self), B, D => old quorum satisfied (A+B), new quorum satisfied (B+D).
	if !n.hasCommitQuorumForIndexLocked(5) {
		t.Fatalf("expected index 5 to satisfy joint quorum")
	}

	// At index 6, only self might have it; should fail.
	if n.hasCommitQuorumForIndexLocked(6) {
		t.Fatalf("expected index 6 to fail joint quorum")
	}
}

func TestComputeRecraftTransitionAdd(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C")
	to := makeSet("A", "B", "C", "D")

	transitionSet, electionQ, replicationQ, err := n.computeRecraftTransitionLocked(from, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sameSet(transitionSet, to) {
		t.Fatalf("expected transition voters to equal target voters for add transition")
	}

	if electionQ != 3 || replicationQ != 3 {
		t.Fatalf("expected add transition quorums to be 3, got election=%d replication=%d", electionQ, replicationQ)
	}
}

func TestComputeRecraftTransitionRemove(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C", "D", "E")
	to := makeSet("A", "B", "C")

	transitionSet, electionQ, replicationQ, err := n.computeRecraftTransitionLocked(from, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sameSet(transitionSet, from) {
		t.Fatalf("expected transition voters to stay on old voters for remove transition")
	}

	if electionQ != 4 || replicationQ != 4 {
		t.Fatalf("expected remove transition quorums to be 4, got election=%d replication=%d", electionQ, replicationQ)
	}
}

func TestComputeRecraftTransitionRejectMixedChange(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C")
	to := makeSet("A", "D", "E")

	_, _, _, err := n.computeRecraftTransitionLocked(from, to)
	if err == nil {
		t.Fatalf("expected mixed change to be rejected")
	}
}

func TestHasElectionQuorumRecraftTransition(t *testing.T) {
	n := &Node{
		reconfigMode:             ReconfigModeRecraft,
		reconfigCurrentPhase:     reconfigPhaseTransition,
		transitionVoterSet:       makeSet("A", "B", "C", "D"),
		transitionElectionQuorum: 3,
	}

	if n.hasElectionQuorumLocked(makeSet("A", "B")) {
		t.Fatalf("expected two votes to be insufficient for recraft transition quorum 3")
	}

	if !n.hasElectionQuorumLocked(makeSet("A", "B", "D")) {
		t.Fatalf("expected three votes to satisfy recraft transition quorum 3")
	}
}

func TestHasCommitQuorumForIndexRecraftTransition(t *testing.T) {
	n := &Node{
		Id:                          "A",
		reconfigMode:                ReconfigModeRecraft,
		reconfigCurrentPhase:        reconfigPhaseTransition,
		transitionVoterSet:          makeSet("A", "B", "C", "D"),
		transitionReplicationQuorum: 3,
		matchIndex: map[string]int64{
			"B": 7,
			"C": 5,
			"D": 7,
		},
	}

	n.db = db.InitMemoryDatabase()
	for i := 0; i <= 7; i++ {
		_, err := n.db.AppendLog(&rcppb.LogEntry{LogType: rcppb.LogType_STORE, Term: 1})
		if err != nil {
			t.Fatalf("failed to append test log %d: %v", i, err)
		}
	}

	if !n.hasCommitQuorumForIndexLocked(7) {
		t.Fatalf("expected index 7 to satisfy recraft transition replication quorum 3")
	}

	if n.hasCommitQuorumForIndexLocked(8) {
		t.Fatalf("expected index 8 to fail recraft transition replication quorum 3")
	}
}
