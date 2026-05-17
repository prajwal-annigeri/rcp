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

func differenceIDsSorted(a map[string]struct{}, b map[string]struct{}) []string {
	ids := make([]string, 0)
	for id := range a {
		if _, ok := b[id]; !ok {
			ids = append(ids, id)
		}
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

func (node *Node) computeOrcaTransitionsLocked(fromSet map[string]struct{}, toSet map[string]struct{}) (map[string]struct{}, int, int, map[string]struct{}, int, int, error) {
	if sameSet(fromSet, toSet) {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: orca transition requires a membership change", ErrInvalidReconfiguration)
	}

	removedIDs := differenceIDsSorted(fromSet, toSet)
	addedIDs := differenceIDsSorted(toSet, fromSet)

	if len(removedIDs) > 0 && len(addedIDs) > 0 {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: orca currently supports pure add or pure remove reconfiguration, got mixed change (added=%d removed=%d)", ErrInvalidReconfiguration, len(addedIDs), len(removedIDs))
	}

	step1Set := cloneSet(fromSet)

	removeCount := (len(removedIDs) + 1) / 2
	addCount := (len(addedIDs) + 1) / 2

	for i := 0; i < removeCount; i++ {
		delete(step1Set, removedIDs[i])
	}
	for i := 0; i < addCount; i++ {
		step1Set[addedIDs[i]] = struct{}{}
	}

	if len(step1Set) == 0 {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: orca step1 produced empty voter set", ErrInvalidReconfiguration)
	}

	oldReplicationQuorum := majorityForSetSize(len(fromSet))
	newMajority := majorityForSetSize(len(toSet))

	// ORCA step 1: resize election quorum first to the new majority while
	// replication still follows the old replication quorum.
	step1ReplicationQuorum := oldReplicationQuorum
	step1ElectionQuorum := newMajority

	if step1ElectionQuorum <= 0 || step1ElectionQuorum > len(step1Set) {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: invalid orca step1 election quorum %d for voter set size %d", ErrInvalidReconfiguration, step1ElectionQuorum, len(step1Set))
	}
	if step1ReplicationQuorum <= 0 || step1ReplicationQuorum > len(step1Set) {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: invalid orca step1 replication quorum %d for voter set size %d", ErrInvalidReconfiguration, step1ReplicationQuorum, len(step1Set))
	}

	step2Set := cloneSet(toSet)
	step2ElectionQuorum := newMajority
	step2ReplicationQuorum := step1ElectionQuorum

	if step2ElectionQuorum <= 0 || step2ElectionQuorum > len(step2Set) {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: invalid orca step2 election quorum %d for voter set size %d", ErrInvalidReconfiguration, step2ElectionQuorum, len(step2Set))
	}
	if step2ReplicationQuorum <= 0 || step2ReplicationQuorum > len(step2Set) {
		return nil, 0, 0, nil, 0, 0, fmt.Errorf("%w: invalid orca step2 replication quorum %d for voter set size %d", ErrInvalidReconfiguration, step2ReplicationQuorum, len(step2Set))
	}

	return step1Set, step1ElectionQuorum, step1ReplicationQuorum, step2Set, step2ElectionQuorum, step2ReplicationQuorum, nil
}

func (node *Node) buildReconfigLogEntriesLocked(targetVoterSet map[string]struct{}) ([]*rcppb.LogEntry, int64, error) {
	if node.reconfigMode == ReconfigModeNone {
		return nil, 0, fmt.Errorf("reconfiguration mode is disabled")
	}

	epoch := node.reconfigEpoch + 1
	fromVoters := sortedIDs(node.activeVoterSet)
	toVoters := sortedIDs(targetVoterSet)

	entries := make([]*rcppb.LogEntry, 0, 4)

	switch node.reconfigMode {
	case ReconfigModeJoint:
		transition := &rcppb.LogEntry{
			LogType: rcppb.LogType_RECONFIG,
			Term:    node.currentTerm,
			Reconfig: &rcppb.ReconfigLog{
				Epoch:            epoch,
				Mode:             node.reconfigMode,
				FromVoters:       fromVoters,
				ToVoters:         toVoters,
				Phase:            rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
				TransitionVoters: sortedIDs(unionSets(node.activeVoterSet, targetVoterSet)),
				TransitionStep:   1,
			},
		}
		entries = append(entries, transition)

	case ReconfigModeRecraft:
		transitionSet, electionQuorum, replicationQuorum, err := node.computeRecraftTransitionLocked(node.activeVoterSet, targetVoterSet)
		if err != nil {
			return nil, 0, err
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
				TransitionVoters:            sortedIDs(transitionSet),
				TransitionElectionQuorum:    int32(electionQuorum),
				TransitionReplicationQuorum: int32(replicationQuorum),
				TransitionStep:              1,
			},
		}
		entries = append(entries, transition)

	case ReconfigModeOrca:
		step1Set, step1ElectionQuorum, step1ReplicationQuorum, step2Set, step2ElectionQuorum, step2ReplicationQuorum, err := node.computeOrcaTransitionsLocked(node.activeVoterSet, targetVoterSet)
		if err != nil {
			return nil, 0, err
		}

		step1 := &rcppb.LogEntry{
			LogType: rcppb.LogType_RECONFIG,
			Term:    node.currentTerm,
			Reconfig: &rcppb.ReconfigLog{
				Epoch:                       epoch,
				Mode:                        node.reconfigMode,
				FromVoters:                  fromVoters,
				ToVoters:                    toVoters,
				Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
				TransitionVoters:            sortedIDs(step1Set),
				TransitionElectionQuorum:    int32(step1ElectionQuorum),
				TransitionReplicationQuorum: int32(step1ReplicationQuorum),
				TransitionStep:              1,
			},
		}

		step2 := &rcppb.LogEntry{
			LogType: rcppb.LogType_RECONFIG,
			Term:    node.currentTerm,
			Reconfig: &rcppb.ReconfigLog{
				Epoch:                       epoch,
				Mode:                        node.reconfigMode,
				FromVoters:                  fromVoters,
				ToVoters:                    toVoters,
				Phase:                       rcppb.ReconfigPhase_RECONFIG_PHASE_TRANSITION,
				TransitionVoters:            sortedIDs(step2Set),
				TransitionElectionQuorum:    int32(step2ElectionQuorum),
				TransitionReplicationQuorum: int32(step2ReplicationQuorum),
				TransitionStep:              2,
			},
		}

		entries = append(entries, step1, step2)
		return entries, epoch, nil

	default:
		return nil, 0, fmt.Errorf("unknown reconfiguration mode %s", node.reconfigMode)
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

	entries = append(entries, finalize)
	return entries, epoch, nil
}

// This function assumes mutex is already locked.
func (node *Node) isTransitionJointPhaseLocked() bool {
	return node.reconfigMode == ReconfigModeJoint && node.reconfigCurrentPhase == reconfigPhaseTransition && len(node.pendingVoterSet) > 0
}

// This function assumes mutex is already locked.
func (node *Node) isTransitionCustomQuorumPhaseLocked() bool {
	if node.reconfigCurrentPhase != reconfigPhaseTransition {
		return false
	}

	if node.reconfigMode != ReconfigModeRecraft && node.reconfigMode != ReconfigModeOrca {
		return false
	}

	return len(node.transitionVoterSet) > 0
}

// This function assumes mutex is already locked.
func (node *Node) isElectionVoterLocked(nodeID string) bool {
	if node.isTransitionJointPhaseLocked() {
		return isMemberOfSet(nodeID, node.activeVoterSet) || isMemberOfSet(nodeID, node.pendingVoterSet)
	}

	if node.isTransitionCustomQuorumPhaseLocked() {
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

	if node.isTransitionCustomQuorumPhaseLocked() {
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

	if node.isTransitionCustomQuorumPhaseLocked() {
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
	node.transitionStep = 0
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
		case ReconfigModeRecraft, ReconfigModeOrca:
			transitionStep := int(payload.TransitionStep)
			if transitionStep <= 0 {
				return fmt.Errorf("invalid %s transition step %d", node.reconfigMode, transitionStep)
			}

			nextTransitionStep := transitionStep
			if node.reconfigMode == ReconfigModeOrca {
				if transitionStep == 1 {
					nextTransitionStep = 1
				} else if transitionStep == 2 {
					if node.transitionStep != 1 {
						return fmt.Errorf("invalid orca transition step order: got step %d with previous step %d", transitionStep, node.transitionStep)
					}
					nextTransitionStep = 2
				} else {
					return fmt.Errorf("invalid orca transition step %d", transitionStep)
				}
			}

			transitionVoterSet := setFromIDs(payload.TransitionVoters)
			if len(transitionVoterSet) == 0 {
				return fmt.Errorf("%s transition voters cannot be empty", node.reconfigMode)
			}
			for nodeID := range transitionVoterSet {
				if _, exists := node.knownNodeSet[nodeID]; !exists {
					return fmt.Errorf("%s transition voters contain unknown node ID: %s", node.reconfigMode, nodeID)
				}
			}

			electionQuorum := int(payload.TransitionElectionQuorum)
			replicationQuorum := int(payload.TransitionReplicationQuorum)
			if electionQuorum <= 0 || electionQuorum > len(transitionVoterSet) {
				return fmt.Errorf("invalid %s election quorum %d for transition voters size %d", node.reconfigMode, electionQuorum, len(transitionVoterSet))
			}
			if replicationQuorum <= 0 || replicationQuorum > len(transitionVoterSet) {
				return fmt.Errorf("invalid %s replication quorum %d for transition voters size %d", node.reconfigMode, replicationQuorum, len(transitionVoterSet))
			}

			node.transitionVoterSet = cloneSet(transitionVoterSet)
			node.transitionElectionQuorum = electionQuorum
			node.transitionReplicationQuorum = replicationQuorum
			node.transitionStep = nextTransitionStep

			// ORCA treats step2 as the terminal step.
			if node.reconfigMode == ReconfigModeOrca && transitionStep == 2 {
				node.activeVoterSet = cloneSet(toVoterSet)
				node.pendingVoterSet = make(map[string]struct{})
				node.resetTransitionStateLocked()

				if !isMemberOfSet(node.Id, node.activeVoterSet) {
					node.StepDownLocked()
					node.votedFor = ""
				}

				node.reconfigInFlight = false
				node.reconfigCurrentPhase = reconfigPhaseStable
			}
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
