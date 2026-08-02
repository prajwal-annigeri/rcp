package node

import (
	"rcp/db"
	"rcp/rcppb"
	"testing"
	"time"
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

func TestComputeOrcaTransitionsPureRemove(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C", "D")
	to := makeSet("A", "B", "C")

	step1Set, step1EQ, step1RQ, step2Set, step2EQ, step2RQ, err := n.computeOrcaTransitionsLocked(from, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sameSet(step1Set, makeSet("A", "B", "C")) {
		t.Fatalf("unexpected orca step1 set: %v", sortedIDs(step1Set))
	}
	if step1EQ != 2 || step1RQ != 3 {
		t.Fatalf("unexpected orca step1 quorums: election=%d replication=%d", step1EQ, step1RQ)
	}
	if !sameSet(step2Set, to) {
		t.Fatalf("expected orca step2 set to match target set")
	}
	if step2EQ != 2 || step2RQ != 2 {
		t.Fatalf("unexpected orca step2 quorums: election=%d replication=%d", step2EQ, step2RQ)
	}
}

func TestComputeOrcaTransitionsPureAdd(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C")
	to := makeSet("A", "B", "C", "D")

	step1Set, step1EQ, step1RQ, step2Set, step2EQ, step2RQ, err := n.computeOrcaTransitionsLocked(from, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sameSet(step1Set, makeSet("A", "B", "C", "D")) {
		t.Fatalf("unexpected orca step1 set: %v", sortedIDs(step1Set))
	}
	if step1EQ != 3 || step1RQ != 2 {
		t.Fatalf("unexpected orca step1 quorums: election=%d replication=%d", step1EQ, step1RQ)
	}
	if !sameSet(step2Set, to) {
		t.Fatalf("expected orca step2 set to match target set")
	}
	if step2EQ != 3 || step2RQ != 3 {
		t.Fatalf("unexpected orca step2 quorums: election=%d replication=%d", step2EQ, step2RQ)
	}
}

func TestComputeOrcaTransitionsRejectMixedChange(t *testing.T) {
	n := &Node{}
	from := makeSet("A", "B", "C")
	to := makeSet("A", "D", "E")

	_, _, _, _, _, _, err := n.computeOrcaTransitionsLocked(from, to)
	if err == nil {
		t.Fatalf("expected mixed change to be rejected")
	}
}

func TestBuildReconfigLogEntriesOrcaIncludesTwoTerminalTransitions(t *testing.T) {
	n := &Node{
		reconfigMode:   ReconfigModeOrca,
		reconfigEpoch:  5,
		currentTerm:    10,
		activeVoterSet: makeSet("A", "B", "C"),
	}

	entries, epoch, err := n.buildReconfigLogEntriesLocked(makeSet("A", "B", "C", "D"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if epoch != 6 {
		t.Fatalf("expected epoch 6, got %d", epoch)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 log entries for orca (step1, step2-terminal), got %d", len(entries))
	}

	if entries[0].GetReconfig().GetTransitionStep() != 1 {
		t.Fatalf("expected first transition step=1")
	}
	if entries[1].GetReconfig().GetTransitionStep() != 2 {
		t.Fatalf("expected second transition step=2")
	}
	if entries[1].GetReconfig().GetPhase() != rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION {
		t.Fatalf("expected second entry to remain transition phase")
	}
}

func TestApplyReconfigTransitionIgnoresStaleMismatchedFromVoters(t *testing.T) {
	n := &Node{
		reconfigMode:         ReconfigModeJoint,
		reconfigCurrentPhase: reconfigPhaseStable,
		reconfigEpoch:        2,
		knownNodeSet:         makeSet("A", "B", "C", "D"),
		activeVoterSet:       makeSet("A", "C", "D"),
		pendingVoterSet:      makeSet(),
		transitionVoterSet:   makeSet(),
	}

	entry := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    1,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:            1,
			Mode:             ReconfigModeJoint,
			FromVoters:       []string{"A", "B", "D"},
			ToVoters:         []string{"A", "C", "D"},
			Phase:            rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
			TransitionVoters: []string{"A", "B", "C", "D"},
			TransitionStep:   1,
		},
	}

	if err := n.applyReconfigLogLocked(entry); err != nil {
		t.Fatalf("unexpected apply error: %v", err)
	}

	if !sameSet(n.activeVoterSet, makeSet("A", "C", "D")) {
		t.Fatalf("expected active voters to remain unchanged for stale mismatch")
	}
	if len(n.pendingVoterSet) != 0 {
		t.Fatalf("expected pending voters to remain empty for stale mismatch")
	}
	if n.reconfigCurrentPhase != reconfigPhaseStable {
		t.Fatalf("expected stable phase, got %s", n.reconfigCurrentPhase)
	}
	if n.reconfigInFlight {
		t.Fatalf("expected no in-flight reconfiguration for stale mismatch")
	}
}

func TestApplyReconfigOrcaStepTwoWithoutStepOneAppliesTerminalState(t *testing.T) {
	n := &Node{
		Id:                   "A",
		reconfigMode:         ReconfigModeOrca,
		reconfigCurrentPhase: reconfigPhaseStable,
		knownNodeSet:         makeSet("A", "B", "C", "D"),
		activeVoterSet:       makeSet("A", "B", "C"),
		pendingVoterSet:      makeSet(),
		transitionVoterSet:   makeSet(),
		stepdownChan:         make(chan struct{}),
		electionTimer:        time.NewTimer(time.Hour),
	}
	t.Cleanup(func() {
		n.electionTimer.Stop()
	})

	step2 := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    1,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:                       1,
			Mode:                        ReconfigModeOrca,
			FromVoters:                  []string{"A", "B", "C"},
			ToVoters:                    []string{"A", "C", "D"},
			Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
			TransitionVoters:            []string{"A", "C", "D"},
			TransitionElectionQuorum:    2,
			TransitionReplicationQuorum: 2,
			TransitionStep:              2,
		},
	}

	if err := n.applyReconfigLogLocked(step2); err != nil {
		t.Fatalf("expected step2-only apply to succeed, got error: %v", err)
	}
	if !sameSet(n.activeVoterSet, makeSet("A", "C", "D")) {
		t.Fatalf("unexpected active voter set after step2-only apply: %v", sortedIDs(n.activeVoterSet))
	}
	if n.reconfigCurrentPhase != reconfigPhaseStable {
		t.Fatalf("expected stable phase after step2-only apply, got %s", n.reconfigCurrentPhase)
	}
	if n.reconfigInFlight {
		t.Fatalf("expected no in-flight reconfiguration after step2-only apply")
	}
}

func TestApplyReconfigOrcaStepTwoFinalizesConfiguration(t *testing.T) {
	n := &Node{
		Id:                          "A",
		reconfigMode:                ReconfigModeOrca,
		reconfigCurrentPhase:        reconfigPhaseStable,
		knownNodeSet:                makeSet("A", "B", "C", "D"),
		activeVoterSet:              makeSet("A", "B", "C"),
		pendingVoterSet:             makeSet(),
		transitionVoterSet:          makeSet(),
		transitionElectionQuorum:    0,
		transitionReplicationQuorum: 0,
		transitionStep:              0,
		stepdownChan:                make(chan struct{}),
		electionTimer:               time.NewTimer(time.Hour),
	}
	t.Cleanup(func() {
		n.electionTimer.Stop()
	})

	step1 := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    1,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:                       1,
			Mode:                        ReconfigModeOrca,
			FromVoters:                  []string{"A", "B", "C"},
			ToVoters:                    []string{"A", "B", "C", "D"},
			Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
			TransitionVoters:            []string{"A", "B", "C", "D"},
			TransitionElectionQuorum:    3,
			TransitionReplicationQuorum: 2,
			TransitionStep:              1,
		},
	}

	step2 := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    1,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:                       1,
			Mode:                        ReconfigModeOrca,
			FromVoters:                  []string{"A", "B", "C"},
			ToVoters:                    []string{"A", "B", "C", "D"},
			Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
			TransitionVoters:            []string{"A", "B", "C", "D"},
			TransitionElectionQuorum:    3,
			TransitionReplicationQuorum: 3,
			TransitionStep:              2,
		},
	}

	if err := n.applyReconfigLogLocked(step1); err != nil {
		t.Fatalf("unexpected step1 apply error: %v", err)
	}
	if err := n.applyReconfigLogLocked(step2); err != nil {
		t.Fatalf("unexpected step2 apply error: %v", err)
	}

	if !sameSet(n.activeVoterSet, makeSet("A", "B", "C", "D")) {
		t.Fatalf("expected active voters to finalize to target set")
	}
	if n.reconfigCurrentPhase != reconfigPhaseStable {
		t.Fatalf("expected reconfig phase stable after orca step2, got %s", n.reconfigCurrentPhase)
	}
	if n.reconfigInFlight {
		t.Fatalf("expected no in-flight reconfiguration after orca step2")
	}
}
