package node

import "time"


func (node *Node) executor() {
	for {
		if node.commitIndex > node.execIndex {
			log, err := node.db.GetLogAtIndex(node.execIndex + 1)
			if err == nil {
				if log.LogType == "store" {
					node.db.PutKV(log.Key, log.Value)
				}
				node.execIndex += 1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
