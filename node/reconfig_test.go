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
