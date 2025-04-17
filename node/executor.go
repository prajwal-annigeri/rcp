package node

import (
	"log"
	"time"
)

// The executor function, runs as a goroutine
func (node *Node) executor() {
	for {
		if node.commitIndex > node.execIndex {
			logEntry, err := node.db.GetLogAtIndex(node.execIndex + 1)
			if err == nil {
				if logEntry.LogType == "store" {
					node.db.PutKV(logEntry.Key, logEntry.Value)
				} else if logEntry.LogType == "failure" {
					node.currAlive -= 1
					node.serverStatusMap.Store(logEntry.NodeId, false)
					go node.removeFromFailureSet(logEntry.NodeId)
				} else if logEntry.LogType == "recovery" {
					node.currAlive += 1
					node.serverStatusMap.Store(logEntry.NodeId, true)
					go node.removeFromRecoverySet(logEntry.NodeId)
				}
				node.execIndex += 1
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}


func (node *Node) callbacker() {
	currIndex := int64(-1)
	for {
		if node.commitIndex > currIndex {
			logEntry, err := node.db.GetLogAtIndex(currIndex + 1)
			if err == nil {
				callbackChannelRaw, ok := node.callbackChannelMap.Load(logEntry.CallbackChannelId)
				if !ok {
					log.Println("BUG: no callback channel")
					continue
				}
				callbackChannel := callbackChannelRaw.(chan struct{})
				callbackChannel <- struct{}{}
				close(callbackChannel)
				currIndex += 1
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
