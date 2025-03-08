package node

import "time"

func (node *Node) executor() {
	for {
		if node.commitIndex > node.execIndex {
			log, err := node.db.GetLogAtIndex(node.execIndex + 1)
			if err == nil {
				if log.LogType == "store" {
					node.db.PutKV(log.Key, log.Value)
				} else if log.LogType == "failure" {
					node.currAlive -= 1
					node.serverStatusMap.Store(log.NodeId, false)
					go node.removeFromFailureSet(log.NodeId)
				} else if log.LogType == "recovery" {
					node.currAlive += 1
					node.serverStatusMap.Store(log.NodeId, true)
					go node.removeFromRecoverySet(log.NodeId)
				}
				node.execIndex += 1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
