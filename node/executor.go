package node

import (
	"log"
)

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

func (node *Node) executeUntil(endIndex int64) error {
	log.Println("Called execute until", endIndex)
	for node.execIndex < endIndex {
		logEntry, err := node.db.GetLogAtIndex(node.execIndex + 1)
		if err != nil {
			log.Printf("No log at index %d", node.execIndex+1)
			continue
		}

		switch logEntry.LogType {
		case "store":
			node.db.Store(logEntry.Key, logEntry.Bucket, logEntry.Value)
		case "delete":
			node.db.Delete(logEntry.Key, logEntry.Bucket)
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
