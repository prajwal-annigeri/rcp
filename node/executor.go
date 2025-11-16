package node

import (
	"log"
	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"

	"google.golang.org/protobuf/proto"
)

// // doCallbacks informs the client that put this log entry about its commit
// // startIndex and endIndex are inclusive
// func (node *Node) doCallbacks(startIndex, endIndex int64) {
// 	for i := startIndex; i <= endIndex; i++ {
// 		go node.doCallback(i)
// 	}
// }

func (node *Node) doCallback(idx int64) {
	callbackChannel, exists := node.indexToCallbackChannelMap[idx]

	if !exists {
		return
	}

	select {
	case callbackChannel <- CallbackReply{"", nil}:
	default:
		// Channel already full or no listener
	}
	close(callbackChannel)

	delete(node.indexToCallbackChannelMap, idx)
}

// This function assume mutex is already locked
func (node *Node) executeUntilLocked(endIndex int64) error {
	log.Printf("Called execute until %d", endIndex)
	for node.execIndex < endIndex {
		logEntry, err := node.db.GetLogAtIndex(node.execIndex + 1)
		if err != nil {
			log.Panicf("No log at index %d", node.execIndex+1)
			continue
		}

		switch logEntry.LogType {
		case orcapb.LogType_OPERATION:
			var req kvpb.KVRequest
			if err := proto.Unmarshal(logEntry.GetPayload(), &req); err != nil {
				log.Panicf("failed to decode operation payload: %v", err)
			}
			switch req.GetOp() {
			case kvpb.OperationType_STORE:
				node.db.Store(req.GetKey(), req.GetBucket(), req.GetValue())
			case kvpb.OperationType_DELETE:
				node.db.Delete(req.GetKey(), req.GetBucket())
			default:
				log.Panicf("unhandled operation type %d", req.GetOp())
			}
			node.doCallback(node.execIndex + 1)
		case orcapb.LogType_FAILURE:
			delete(node.pendingFailureSet, logEntry.NodeId)
			node.failedSet[logEntry.NodeId] = struct{}{}
		case orcapb.LogType_RECOVERY:
			delete(node.pendingRecoverySet, logEntry.NodeId)
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
