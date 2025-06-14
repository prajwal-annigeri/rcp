package node

import (
	"encoding/json"
	"fmt"
	"rcp/constants"
	"rcp/db"
	"rcp/rcppb"
	"time"

	bolt "go.etcd.io/bbolt"
)

// The executor function, runs as a goroutine
func (node *Node) executor() {
	for {
		if node.commitIndex > node.execIndex {
			logEntry, err := node.db.GetLogAtIndex(node.execIndex + 1)
			if err == nil {
				if logEntry.LogType == "store" {
					node.db.PutKV(logEntry.Key, logEntry.Value, logEntry.Bucket)
				} else if logEntry.LogType == "delete" {
					node.db.DeleteKV(logEntry.Key, logEntry.Bucket)
				} else if logEntry.LogType == "failure" {
					node.currAlive -= 1
					node.serverStatusMap.Store(logEntry.NodeId, false)
					go node.removeFromFailureSet(logEntry.NodeId)
				} else if logEntry.LogType == "recovery" {
					node.currAlive += 1
					node.serverStatusMap.Store(logEntry.NodeId, true)
					go node.removeFromRecoverySet(logEntry.NodeId)
				} else if logEntry.LogType == "bank" {
					node.db.ModifyBalance(logEntry.Transaction1.AccountId, db.AccountType(logEntry.Transaction1.AccountType), logEntry.Transaction1.Amount)
					if logEntry.Transaction2 != nil {
						node.db.ModifyBalance(logEntry.Transaction2.AccountId, db.AccountType(logEntry.Transaction2.AccountType), logEntry.Transaction2.Amount)
					}
				}
				node.execIndex += 1
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Performs callback to let client know log entry is committed
func (node *Node) callbacker() {
	currIndex := int64(-1)
	for {
		currCommit := node.commitIndex
		if currIndex == currCommit {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		node.db.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(constants.LogsBucket)
			for currIndex < currCommit {
				currIndex++
				logBytes := b.Get(fmt.Appendf(nil, "%d", currIndex))
				if logBytes == nil {
					return fmt.Errorf("callbacker(): error empty log at index %d", currIndex)
				}
				var logEntry rcppb.LogEntry
				err := json.Unmarshal(logBytes, &logEntry)
				if err != nil {
					return err
				}
				callbackChannelRaw, ok := node.callbackChannelMap.Load(logEntry.CallbackChannelId)
				if !ok {
					// log.Println("no callback channel for index %d", currIndex)
					return nil
				}
				callbackChannel := callbackChannelRaw.(chan struct{})
				callbackChannel <- struct{}{}
				close(callbackChannel)
			}
			
			return nil

		})
		// if node.commitIndex > currIndex {
		// 	currIndex += 1
		// 	go node.doCallback(currIndex)
		// }
		// time.Sleep(2 * time.Millisecond)
	}
}

// func (node *Node) doCallback(index int64) {
// 	begin := time.Now()
// 	logEntry, err := node.db.GetLogAtIndex(index)
// 	if err == nil {
// 		callbackChannelRaw, ok := node.callbackChannelMap.Load(logEntry.CallbackChannelId)
// 		if !ok {
// 			// log.Println("no callback channel")
// 			return
// 		}
// 		callbackChannel := callbackChannelRaw.(chan struct{})
// 		callbackChannel <- struct{}{}
// 		log.Printf("Time for callback: %v", time.Since(begin))
// 		close(callbackChannel)
// 	}
// }
