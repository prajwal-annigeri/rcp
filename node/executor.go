package node

import (
	"encoding/json"
	"fmt"
	"log"
	"rcp/constants"
	"rcp/rcppb"

	bolt "go.etcd.io/bbolt"
)

// // The executor function, runs as a goroutine
// func (node *Node) executor() {

// 	for {
// 		currCommit := node.commitIndex
// 		if currCommit > node.execIndex {
// 			if node.isPersistent {
// 				err := node.persistentExecuteTill(currCommit)
// 				if err != nil {
// 					log.Printf("Error executing: %v", err)
// 				}
// 			} else {
// 				err := node.inMemoryExecuteTill(currCommit)
// 				if err != nil {
// 					log.Printf("Error executing: %v", err)
// 				}
// 			}

// 		}
// 		// if node.commitIndex > node.execIndex {
// 		// 	logEntry, err := node.db.GetLogAtIndex(node.execIndex + 1)
// 		// 	if err == nil {
// 		// 		if logEntry.LogType == "store" {
// 		// 			node.db.PutKV(logEntry.Key, logEntry.Value, logEntry.Bucket)
// 		// 		} else if logEntry.LogType == "delete" {
// 		// 			node.db.DeleteKV(logEntry.Key, logEntry.Bucket)
// 		// 		} else if logEntry.LogType == "failure" {
// 		// 			node.currAlive -= 1
// 		// 			node.serverStatusMap.Store(logEntry.NodeId, false)
// 		// 			go node.removeFromFailureSet(logEntry.NodeId)
// 		// 		} else if logEntry.LogType == "recovery" {
// 		// 			node.currAlive += 1
// 		// 			node.serverStatusMap.Store(logEntry.NodeId, true)
// 		// 			go node.removeFromRecoverySet(logEntry.NodeId)
// 		// 		} else if logEntry.LogType == "bank" {
// 		// 			node.db.ModifyBalance(logEntry.Transaction1.AccountId, db.AccountType(logEntry.Transaction1.AccountType), logEntry.Transaction1.Amount)
// 		// 			if logEntry.Transaction2 != nil {
// 		// 				node.db.ModifyBalance(logEntry.Transaction2.AccountId, db.AccountType(logEntry.Transaction2.AccountType), logEntry.Transaction2.Amount)
// 		// 			}
// 		// 		}
// 		// 		node.execIndex += 1
// 		// 	}
// 		// }
// 		time.Sleep(2 * time.Millisecond)
// 	}
// }

// doCallbacks informs the client that put this log entry about its commit
// startIndex and endIndex are inclusive
func (node *Node) doCallbacks(startIndex, endIndex int64) {
	for i := startIndex; i <= endIndex; i++ {
		go node.doCallback(i)
	}
}

func (node *Node) doCallback(idx int64) {
	callbackChannelRaw, ok := node.indexToCallbackChannelMap.Load(idx)
	if !ok {
		// log.Println("no callback channel for index %d", currIndex)
		return
	}
	callbackChannel := callbackChannelRaw.(chan struct{})
	callbackChannel <- struct{}{}
	// log.Printf("LOGX time doCallback: %v", time.Now().UnixMilli())
	close(callbackChannel)
}

// This method executes logs from node.execIndex + 1 to endIndex
// This method is used in the persistent mode
func (node *Node) persistentExecuteTill(endIndex int64) error {
	return node.db.DB.Update(func(tx *bolt.Tx) error {
		logsBkt := tx.Bucket(constants.LogsBucket)
		for node.execIndex < endIndex {
			logBytes := logsBkt.Get(fmt.Appendf(nil, "%d", node.execIndex+1))
			if logBytes == nil {
				return fmt.Errorf("executor() no log at index: %d", node.execIndex+1)
			}
			var logEntry rcppb.LogEntry
			err := json.Unmarshal(logBytes, &logEntry)
			if err != nil {
				return fmt.Errorf("could not deserialize logbytes of index %d into logentry: %v", node.execIndex+1, err)
			}

			switch logEntry.LogType {
			case "store":
				kvBkt := tx.Bucket([]byte(logEntry.Bucket))
				if kvBkt == nil {
					return fmt.Errorf("executor() index %d, no bucket %s", node.execIndex+1, logEntry.Bucket)
				}
				kvBkt.Put([]byte(logEntry.Key), []byte(logEntry.Value))
			case "delete":
				kvBkt := tx.Bucket([]byte(logEntry.Bucket))
				if kvBkt == nil {
					return fmt.Errorf("executor() index %d, no bucket %s", node.execIndex+1, logEntry.Bucket)
				}
				kvBkt.Delete([]byte(logEntry.Key))
			case "failure":
				node.currAlive -= 1
				node.serverStatusMap.Store(logEntry.NodeId, false)
				go node.removeFromFailureSet(logEntry.NodeId)
			case "recovery":
				node.currAlive += 1
				node.serverStatusMap.Store(logEntry.NodeId, true)
				go node.removeFromRecoverySet(logEntry.NodeId)
			}

			node.execIndex++
		}
		return nil
	})
}

func (node *Node) inMemoryExecuteTill(endIndex int64) error {
	for node.execIndex < endIndex {
		logEntry, err := node.GetInMemoryLog(node.execIndex + 1)
		if err != nil {
			log.Printf("executor() No log at index %d", node.execIndex+1)
			continue
		}

		switch logEntry.LogType {
		case "store":
			node.inMemoryKV.Store(fmt.Sprintf("%s/%s", logEntry.Bucket, logEntry.Key), logEntry.Value)
		case "delete":
			node.inMemoryKV.Delete(fmt.Sprintf("%s/%s", logEntry.Bucket, logEntry.Key))
		case "failure":
			node.currAlive -= 1
			node.serverStatusMap.Store(logEntry.NodeId, false)
			go node.removeFromFailureSet(logEntry.NodeId)
		case "recovery":
			node.currAlive += 1
			node.serverStatusMap.Store(logEntry.NodeId, true)
			go node.removeFromRecoverySet(logEntry.NodeId)
		}

		node.execIndex++
	}
	return nil
}

// Performs callback to let client know log entry is committed
// func (node *Node) callbacker() {
// 	currIndex := int64(-1)
// 	for {
// 		currCommit := node.commitIndex
// 		if currIndex == currCommit {
// 			time.Sleep(2 * time.Millisecond)
// 			continue
// 		}
// 		node.db.DB.View(func(tx *bolt.Tx) error {
// 			b := tx.Bucket(constants.LogsBucket)
// 			for currIndex < currCommit {
// 				currIndex++
// 				logBytes := b.Get(fmt.Appendf(nil, "%d", currIndex))
// 				if logBytes == nil {
// 					return fmt.Errorf("callbacker(): error empty log at index %d", currIndex)
// 				}
// 				var logEntry rcppb.LogEntry
// 				err := json.Unmarshal(logBytes, &logEntry)
// 				if err != nil {
// 					return err
// 				}
// 				callbackChannelRaw, ok := node.callbackChannelMap.Load(logEntry.CallbackChannelId)
// 				if !ok {
// 					// log.Println("no callback channel for index %d", currIndex)
// 					return nil
// 				}
// 				callbackChannel := callbackChannelRaw.(chan struct{})
// 				callbackChannel <- struct{}{}
// 				close(callbackChannel)
// 			}

// 			return nil

// 		})
// if node.commitIndex > currIndex {
// 	currIndex += 1
// 	go node.doCallback(currIndex)
// }
// time.Sleep(2 * time.Millisecond)
// 	}
// }

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
