package node

import (
	"log"
	"rcp/grpc/kvpb"
	"rcp/grpc/orcapb"

	"google.golang.org/protobuf/proto"
)

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
