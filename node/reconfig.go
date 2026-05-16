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

func unionSets(a map[string]struct{}, b map[string]struct{}) map[string]struct{} {
	union := cloneSet(a)
	for id := range b {
		union[id] = struct{}{}
	}
	return union
}

func setDifferenceCount(a map[string]struct{}, b map[string]struct{}) int {
	diff := 0
	for id := range a {
		if _, ok := b[id]; !ok {
			diff++
		}
	}
	return diff
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

func majorityForSetSize(size int) int {
	return (size / 2) + 1
}

func isMemberOfSet(nodeID string, set map[string]struct{}) bool {
	_, ok := set[nodeID]
	return ok
}

func countSetMembershipLocked(voters map[string]struct{}, population map[string]struct{}) int {
	count := 0
	for voterID := range voters {
		if isMemberOfSet(voterID, population) {
			count++
		}
	}
	return count
}

func (node *Node) computeRecraftTransitionLocked(fromSet map[string]struct{}, toSet map[string]struct{}) (map[string]struct{}, int, int, error) {
	added := setDifferenceCount(toSet, fromSet)
	removed := setDifferenceCount(fromSet, toSet)

	if added > 0 && removed > 0 {
		return nil, 0, 0, fmt.Errorf("%w: recraft currently supports pure add or pure remove reconfiguration, got mixed change (added=%d removed=%d)", ErrInvalidReconfiguration, added, removed)
	}

	if added == 0 && removed == 0 {
		return nil, 0, 0, fmt.Errorf("%w: recraft transition requires a membership change", ErrInvalidReconfiguration)
	}

	qOld := majorityForSetSize(len(fromSet))
	qNew := majorityForSetSize(len(toSet))

	if added > 0 {
		// AddAndResize: transition members are final members, with enlarged quorum.
		qTransition := added + len(fromSet) - qOld + 1
		transitionSet := cloneSet(toSet)
		if qTransition <= 0 || qTransition > len(transitionSet) {
			return nil, 0, 0, fmt.Errorf("%w: invalid recraft add transition quorum=%d for transition voter set size=%d", ErrInvalidReconfiguration, qTransition, len(transitionSet))
		}
		return transitionSet, qTransition, qTransition, nil
	}

	// RemoveAndResize: transition members stay on old membership, with enlarged quorum.
	qTransition := len(fromSet) - qNew + 1
	transitionSet := cloneSet(fromSet)
	if qTransition <= 0 || qTransition > len(transitionSet) {
		return nil, 0, 0, fmt.Errorf("%w: invalid recraft remove transition quorum=%d for transition voter set size=%d", ErrInvalidReconfiguration, qTransition, len(transitionSet))
	}
	return transitionSet, qTransition, qTransition, nil
}

func (node *Node) buildReconfigLogEntriesLocked(targetVoterSet map[string]struct{}) ([]*rcppb.LogEntry, int64, error) {
	if node.reconfigMode == ReconfigModeNone {
		return nil, 0, fmt.Errorf("reconfiguration mode is disabled")
	}

	epoch := node.reconfigEpoch + 1
	fromVoters := sortedIDs(node.activeVoterSet)
	toVoters := sortedIDs(targetVoterSet)

	var transitionVoters []string
	var transitionElectionQuorum int
	var transitionReplicationQuorum int

	switch node.reconfigMode {
	case ReconfigModeJoint:
		transitionVoters = sortedIDs(unionSets(node.activeVoterSet, targetVoterSet))
	case ReconfigModeRecraft:
		transitionSet, electionQuorum, replicationQuorum, err := node.computeRecraftTransitionLocked(node.activeVoterSet, targetVoterSet)
		if err != nil {
			return nil, 0, err
		}
		transitionVoters = sortedIDs(transitionSet)
		transitionElectionQuorum = electionQuorum
		transitionReplicationQuorum = replicationQuorum
	case ReconfigModeOrca:
		// ORCA phase-specific quorum behavior is implemented in a later phase.
		transitionVoters = sortedIDs(unionSets(node.activeVoterSet, targetVoterSet))
	default:
		return nil, 0, fmt.Errorf("unknown reconfiguration mode %s", node.reconfigMode)
	}

	transition := &rcppb.LogEntry{
		LogType: rcppb.LogType_RECONFIG,
		Term:    node.currentTerm,
		Reconfig: &rcppb.ReconfigLog{
			Epoch:                       epoch,
			Mode:                        node.reconfigMode,
			FromVoters:                  fromVoters,
			ToVoters:                    toVoters,
			Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
			TransitionVoters:            transitionVoters,
			TransitionElectionQuorum:    int32(transitionElectionQuorum),
			TransitionReplicationQuorum: int32(transitionReplicationQuorum),
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

// This function assumes mutex is already locked.
func (node *Node) isTransitionJointPhaseLocked() bool {
	return node.reconfigMode == ReconfigModeJoint && node.reconfigCurrentPhase == reconfigPhaseTransition && len(node.pendingVoterSet) > 0
}

// This function assumes mutex is already locked.
func (node *Node) isTransitionRecraftPhaseLocked() bool {
	return node.reconfigMode == ReconfigModeRecraft && node.reconfigCurrentPhase == reconfigPhaseTransition && len(node.transitionVoterSet) > 0
}

// This function assumes mutex is already locked.
func (node *Node) isElectionVoterLocked(nodeID string) bool {
	if node.isTransitionJointPhaseLocked() {
		return isMemberOfSet(nodeID, node.activeVoterSet) || isMemberOfSet(nodeID, node.pendingVoterSet)
	}

	if node.isTransitionRecraftPhaseLocked() {
		return isMemberOfSet(nodeID, node.transitionVoterSet)
	}

	return isMemberOfSet(nodeID, node.activeVoterSet)
}

// This function assumes mutex is already locked.
func (node *Node) hasElectionQuorumLocked(votes map[string]struct{}) bool {
	if node.isTransitionJointPhaseLocked() {
		oldVotes := countSetMembershipLocked(votes, node.activeVoterSet)
		newVotes := countSetMembershipLocked(votes, node.pendingVoterSet)
		return oldVotes >= majorityForSetSize(len(node.activeVoterSet)) && newVotes >= majorityForSetSize(len(node.pendingVoterSet))
	}

	if node.isTransitionRecraftPhaseLocked() {
		if node.transitionElectionQuorum <= 0 {
			return false
		}
		transitionVotes := countSetMembershipLocked(votes, node.transitionVoterSet)
		return transitionVotes >= node.transitionElectionQuorum
	}

	stableVotes := countSetMembershipLocked(votes, node.activeVoterSet)
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

func countReplicatedInSetLocked(node *Node, voterSet map[string]struct{}, index int64) int {
	count := 0
	for voterID := range voterSet {
		if node.isReplicatedOnVoterLocked(voterID, index) {
			count++
		}
	}
	return count
}

// This function assumes mutex is already locked.
func (node *Node) hasCommitQuorumForIndexLocked(index int64) bool {
	if node.isTransitionJointPhaseLocked() {
		oldReplicated := countReplicatedInSetLocked(node, node.activeVoterSet, index)
		newReplicated := countReplicatedInSetLocked(node, node.pendingVoterSet, index)
		return oldReplicated >= majorityForSetSize(len(node.activeVoterSet)) && newReplicated >= majorityForSetSize(len(node.pendingVoterSet))
	}

	if node.isTransitionRecraftPhaseLocked() {
		if node.transitionReplicationQuorum <= 0 {
			return false
		}
		replicated := countReplicatedInSetLocked(node, node.transitionVoterSet, index)
		return replicated >= node.transitionReplicationQuorum
	}

	replicated := countReplicatedInSetLocked(node, node.activeVoterSet, index)
	return replicated >= majorityForSetSize(len(node.activeVoterSet))
}

// This function assumes mutex is already locked.
func (node *Node) resetTransitionStateLocked() {
	node.transitionVoterSet = make(map[string]struct{})
	node.transitionElectionQuorum = 0
	node.transitionReplicationQuorum = 0
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

	fromVoterSet := setFromIDs(payload.FromVoters)
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
		if len(fromVoterSet) > 0 && !sameSet(node.activeVoterSet, fromVoterSet) {
			return fmt.Errorf("reconfig transition from-voters do not match active voters")
		}

		node.reconfigInFlight = true
		node.reconfigEpoch = max(node.reconfigEpoch, payload.Epoch)
		node.pendingVoterSet = cloneSet(toVoterSet)
		node.reconfigCurrentPhase = reconfigPhaseTransition

		switch node.reconfigMode {
		case ReconfigModeJoint:
			node.resetTransitionStateLocked()
		case ReconfigModeRecraft:
			transitionVoterSet := setFromIDs(payload.TransitionVoters)
			if len(transitionVoterSet) == 0 {
				return fmt.Errorf("recraft transition voters cannot be empty")
			}
			for nodeID := range transitionVoterSet {
				if _, exists := node.knownNodeSet[nodeID]; !exists {
					return fmt.Errorf("recraft transition voters contain unknown node ID: %s", nodeID)
				}
			}

			electionQuorum := int(payload.TransitionElectionQuorum)
			replicationQuorum := int(payload.TransitionReplicationQuorum)
			if electionQuorum <= 0 || electionQuorum > len(transitionVoterSet) {
				return fmt.Errorf("invalid recraft election quorum %d for transition voters size %d", electionQuorum, len(transitionVoterSet))
			}
			if replicationQuorum <= 0 || replicationQuorum > len(transitionVoterSet) {
				return fmt.Errorf("invalid recraft replication quorum %d for transition voters size %d", replicationQuorum, len(transitionVoterSet))
			}

			node.transitionVoterSet = cloneSet(transitionVoterSet)
			node.transitionElectionQuorum = electionQuorum
			node.transitionReplicationQuorum = replicationQuorum
		case ReconfigModeOrca:
			node.resetTransitionStateLocked()
		default:
			return fmt.Errorf("unknown reconfiguration mode: %s", node.reconfigMode)
		}

		return nil

	case rcppb.ReconfigPhase_RECONFIG_PHASE_FINALIZE:
		node.reconfigInFlight = true
		node.reconfigEpoch = max(node.reconfigEpoch, payload.Epoch)
		node.reconfigCurrentPhase = reconfigPhaseFinalizing
		node.activeVoterSet = cloneSet(toVoterSet)
		node.pendingVoterSet = make(map[string]struct{})
		node.resetTransitionStateLocked()

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
