package node

import (
	"fmt"
	"rcp/rcppb"
	"slices"
)

const (
	reconfigPhaseStable     = "stable"
	reconfigPhaseTransition = "transition"
	reconfigPhaseFinalizing = "finalizing"
)

func cloneSet(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for id := range src {
		dst[id] = struct{}{}
	}
	return dst
}

func setFromIDs(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func sortedIDs(set map[string]struct{}) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func sameSet(a map[string]struct{}, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

func (node *Node) buildReconfigLogEntriesLocked(targetVoterSet map[string]struct{}) ([]*rcppb.LogEntry, int64, error) {
	if node.reconfigMode == ReconfigModeNone {
		return nil, 0, fmt.Errorf("reconfiguration mode is disabled")
	}

	epoch := node.reconfigEpoch + 1
	fromVoters := sortedIDs(node.activeVoterSet)
	toVoters := sortedIDs(targetVoterSet)

	transition := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    node.currentTerm,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:      epoch,
			Mode:       node.reconfigMode,
			FromVoters: fromVoters,
			ToVoters:   toVoters,
			Phase:      rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
		},
	}

	finalize := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    node.currentTerm,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:      epoch,
			Mode:       node.reconfigMode,
			FromVoters: fromVoters,
			ToVoters:   toVoters,
			Phase:      rcppb.ReconfigPhase_RECONFIG_PHASE_FINALIZE,
		},
	}

	return []*rcppb.LogEntry{transition, finalize}, epoch, nil
}

func majorityForSetSize(size int) int {
	return (size / 2) + 1
}

func isMemberOfSet(nodeID string, set map[string]struct{}) bool {
	_, ok := set[nodeID]
	return ok
}

// This function assumes mutex is already locked.
func (node *Node) isTransitionJointPhaseLocked() bool {
	return node.reconfigMode == ReconfigModeJoint && node.reconfigCurrentPhase == reconfigPhaseTransition && len(node.pendingVoterSet) > 0
}

// This function assumes mutex is already locked.
func (node *Node) isElectionVoterLocked(nodeID string) bool {
	if node.isTransitionJointPhaseLocked() {
		return isMemberOfSet(nodeID, node.activeVoterSet) || isMemberOfSet(nodeID, node.pendingVoterSet)
	}

	return isMemberOfSet(nodeID, node.activeVoterSet)
}

// This function assumes mutex is already locked.
func (node *Node) hasElectionQuorumLocked(votes map[string]struct{}) bool {
	if node.isTransitionJointPhaseLocked() {
		oldVotes := 0
		newVotes := 0

		for voterID := range votes {
			if isMemberOfSet(voterID, node.activeVoterSet) {
				oldVotes++
			}
			if isMemberOfSet(voterID, node.pendingVoterSet) {
				newVotes++
			}
		}

		return oldVotes >= majorityForSetSize(len(node.activeVoterSet)) && newVotes >= majorityForSetSize(len(node.pendingVoterSet))
	}

	stableVotes := 0
	for voterID := range votes {
		if isMemberOfSet(voterID, node.activeVoterSet) {
			stableVotes++
		}
	}

	return stableVotes >= majorityForSetSize(len(node.activeVoterSet))
}

// This function assumes mutex is already locked.
func (node *Node) isReplicatedOnVoterLocked(voterID string, index int64) bool {
	if voterID == node.Id {
		return node.GetLastIndexLocked() >= index
	}

	matchIdx, ok := node.matchIndex[voterID]
	return ok && matchIdx >= index
}

// This function assumes mutex is already locked.
func (node *Node) hasCommitQuorumForIndexLocked(index int64) bool {
	if node.isTransitionJointPhaseLocked() {
		oldReplicated := 0
		newReplicated := 0

		for voterID := range node.activeVoterSet {
			if node.isReplicatedOnVoterLocked(voterID, index) {
				oldReplicated++
			}
		}

		for voterID := range node.pendingVoterSet {
			if node.isReplicatedOnVoterLocked(voterID, index) {
				newReplicated++
			}
		}

		return oldReplicated >= majorityForSetSize(len(node.activeVoterSet)) && newReplicated >= majorityForSetSize(len(node.pendingVoterSet))
	}

	replicated := 0
	for voterID := range node.activeVoterSet {
		if node.isReplicatedOnVoterLocked(voterID, index) {
			replicated++
		}
	}

	return replicated >= majorityForSetSize(len(node.activeVoterSet))
}

// This function assumes mutex is already locked.
func (node *Node) applyReconfigLogLocked(logEntry *rcppb.LogEntry) error {
	if logEntry.Reconfig == nil {
		return fmt.Errorf("reconfig log has nil payload")
	}

	payload := logEntry.Reconfig

	if payload.Mode != node.reconfigMode {
		return fmt.Errorf("reconfig mode mismatch: local=%s log=%s", node.reconfigMode, payload.Mode)
	}

	toVoterSet := setFromIDs(payload.ToVoters)
	for nodeID := range toVoterSet {
		if _, exists := node.knownNodeSet[nodeID]; !exists {
			return fmt.Errorf("reconfig target contains unknown node ID: %s", nodeID)
		}
	}

	if len(toVoterSet) == 0 {
		return fmt.Errorf("reconfig target voter set is empty")
	}

	switch payload.Phase {
	case rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION:
		node.reconfigInFlight = true
		node.reconfigEpoch = max(node.reconfigEpoch, payload.Epoch)
		node.pendingVoterSet = cloneSet(toVoterSet)
		node.reconfigCurrentPhase = reconfigPhaseTransition
		return nil

	case rcppb.ReconfigPhase_RECONFIG_PHASE_FINALIZE:
		node.reconfigInFlight = true
		node.reconfigEpoch = max(node.reconfigEpoch, payload.Epoch)
		node.reconfigCurrentPhase = reconfigPhaseFinalizing
		node.activeVoterSet = cloneSet(toVoterSet)
		node.pendingVoterSet = make(map[string]struct{})

		if !isMemberOfSet(node.Id, node.activeVoterSet) {
			node.StepDownLocked()
			node.votedFor = ""
		}

		node.reconfigInFlight = false
		node.reconfigCurrentPhase = reconfigPhaseStable
		return nil

	default:
		return fmt.Errorf("unknown reconfig phase: %v", payload.Phase)
	}
}
