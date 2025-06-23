package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"rcp/constants"
	"rcp/rcppb"

	bolt "go.etcd.io/bbolt"
)


func (node *Node) insertLogsPersistent(appendEntryReq *rcppb.AppendEntriesReq) error {
	currIndex := appendEntryReq.PrevLogIndex + 1
	lastEntryTerm := appendEntryReq.Term
	return node.db.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("bucket not found 'logs'")
		}
		for _, entry := range appendEntryReq.Entries {
			// Lookup existing log at index
			key := fmt.Appendf(nil, "%d", currIndex)

			if _, ok := node.possibleFailureOrRecoveryIndex.Load(currIndex); ok {
				logBytes := b.Get(key)

				// Track removed nodes
				if logBytes != nil {
					var existingEntry rcppb.LogEntry
					err := json.Unmarshal(logBytes, &existingEntry)
					if err != nil {
						return fmt.Errorf("could not deserialize existing log at index %d: %v", currIndex, err)
					}

					if existingEntry.LogType == "failure" {
						node.removeFromFailureSet(existingEntry.NodeId)
					} else if existingEntry.LogType == "recovery" {
						node.removeFromRecoverySet(existingEntry.NodeId)
					}
				}
			}

			// Marshal and put new entry
			if entry.LogType == "failure" || entry.LogType == "success" {
				go node.possibleFailureOrRecoveryIndex.Store(currIndex, "")
			}
			newLogBytes, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("could not marshal new log entry at index %d: %v", currIndex, err)
			}
			err = b.Put(key, newLogBytes)
			if err != nil {
				return fmt.Errorf("failed to write log at index %d: %v", currIndex, err)
			}

			node.lastIndex = currIndex
			lastEntryTerm = entry.Term
			currIndex++
		}

		node.lastTerm = lastEntryTerm
		return nil
	})
}

func (node *Node) insertLogsInMemory(appendEntryReq *rcppb.AppendEntriesReq) error {
	currIndex := appendEntryReq.PrevLogIndex + 1
	lastEntryTerm := appendEntryReq.Term
	for _, entry := range appendEntryReq.Entries {
			// Lookup existing log at index

			if _, ok := node.possibleFailureOrRecoveryIndex.Load(currIndex); ok {
				existingEntry, err := node.GetInMemoryLog(currIndex)
				if err == nil {
					if existingEntry.LogType == "failure" {
						node.removeFromFailureSet(existingEntry.NodeId)
					} else if existingEntry.LogType == "recovery" {
						node.removeFromRecoverySet(existingEntry.NodeId)
					}
				}
				
			}

			// Put new entry
			if entry.LogType == "failure" || entry.LogType == "success" {
				go node.possibleFailureOrRecoveryIndex.Store(currIndex, "")
			}
			node.PutInMemoryLog(currIndex, entry)
			node.lastIndex = currIndex
			lastEntryTerm = entry.Term
			currIndex++
		}

		node.lastTerm = lastEntryTerm
		return nil
}

func (node *Node) GetInMemoryLog(index int64) (*rcppb.LogEntry, error) {
	logEntry, ok := node.inMemoryLogs.Load(index)
	if !ok {
		return nil, fmt.Errorf("no log at index %d", index)
	}

	return logEntry.(*rcppb.LogEntry), nil
}

func (node *Node) PutInMemoryLog(index int64, logEntry *rcppb.LogEntry) {
	node.inMemoryLogs.Store(index, logEntry)
}

func (node *Node) PrintAllInMemoryLogs() {
	for i := int64(0); i <= node.lastIndex; i++ {
		logEntryRaw, ok := node.inMemoryLogs.Load(i)
		if !ok {
			log.Printf("No log at index %d", i)
			continue
		}

		logEntry := logEntryRaw.(*rcppb.LogEntry)
		if logEntry.Key != "" {
			log.Printf("%d. Key: %s Bucket: %s\n", i+1, logEntry.Key, logEntry.Bucket)
		} else {
			log.Printf("%d. %s %s", i+1, logEntry.LogType, logEntry.NodeId)
		}
		
	}
}

func (node *Node) GetInMemoryLogsFromIndex(index int64) ([]*rcppb.LogEntry, error) {
	var logsSlice []*rcppb.LogEntry
	for i := index; ; i++ {
		logEntryRaw, ok := node.inMemoryLogs.Load(i)
		if !ok {
			break
		}
		logsSlice = append(logsSlice, logEntryRaw.(*rcppb.LogEntry))
	}

	return logsSlice, nil
}
