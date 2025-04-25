package node

import (
	"log"
	"rcp/db"
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
				} else if logEntry.LogType == "bank" {
					node.db.ModifyBalance(logEntry.Transaction1.AccountId, db.AccountType(logEntry.Transaction1.AccountType), logEntry.Transaction1.Amount)
					if logEntry.Transaction2 != nil {
						node.db.ModifyBalance(logEntry.Transaction2.AccountId, db.AccountType(logEntry.Transaction2.AccountType), logEntry.Transaction2.Amount)
					}
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
			currIndex += 1
			go node.doCallback(currIndex)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (node *Node) doCallback(index int64) {
	logEntry, err := node.db.GetLogAtIndex(index)
	if err == nil {
		callbackChannelRaw, ok := node.callbackChannelMap.Load(logEntry.CallbackChannelId)
		if !ok {
			log.Println("no callback channel")
			return
		}
		callbackChannel := callbackChannelRaw.(chan struct{})
		callbackChannel <- struct{}{}
		close(callbackChannel)
	}
}
