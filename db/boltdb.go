package db

import (
	"fmt"
	"log"
	"rcp/constants"
	"rcp/rcppb"

	bolt "go.etcd.io/bbolt"
)

type LogType int

const (
	Store LogType = iota
	Failure
	Recovery
)

type BoltDB struct {
	DB *bolt.DB
}

// Initialize boltDB
func InitBoltDatabase(dbPath string) (db *BoltDB, closeFunc func() error, err error) {
	log.Println("Initializing store")
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}

	db = &BoltDB{
		DB: boltDB,
	}

	if err := db.createBuckets(); err != nil {
		boltDB.Close()
		panic(err)
	}

	return db, boltDB.Close, nil
}

// Get implements Database.
func (d *BoltDB) Get(key string, bucket string) (string, error) {
	var value string
	log.Printf("Getting key '%s' from bucket '%s'", key, bucket)
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			log.Printf("Bucket %s does not exist", bucket)
			return fmt.Errorf("bucket %s does not exist", bucket)
		}
		valueBytes := b.Get([]byte(key))
		if valueBytes == nil {
			return fmt.Errorf("no value for key %s", key)
		}
		value = string(valueBytes)
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

// Store implements Database.
func (d *BoltDB) Store(key string, bucket string, value string) error {
	panic("unimplemented")
	// log.Printf("Storing key %s to bucket %s\n", key, bucket)
	// return d.DB.Batch(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket([]byte(bucket))
	// 	if b == nil {
	// 		log.Printf("Bucket %s does not exist", bucket)
	// 		return fmt.Errorf("bucket %s does not exist", bucket)
	// 	}
	// 	return b.Put([]byte(key), []byte(value))
	// })
}

// Delete implements Database.
func (d *BoltDB) Delete(key string, bucket string) error {
	panic("unimplemented")
	// log.Printf("Deleting key: %s bucket: %s", key, bucket)
	// return d.DB.Batch(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket([]byte(bucket))
	// 	if b == nil {
	// 		return fmt.Errorf("bucket %s does not exist", bucket)
	// 	}
	// 	err := b.Delete([]byte(key))
	// 	if err != nil {
	// 		return fmt.Errorf("error deleting key %s from bucket %s: %v", key, bucket, err)
	// 	}
	// 	return nil
	// })
}

// PutLogAtIndex implements Database.
func (d *BoltDB) PutLogAtIndex(index int64, log *rcppb.LogEntry) error {
	panic("unimplemented")
}

// GetLogAtIndex implements Database.
func (d *BoltDB) GetLogAtIndex(index int64) (*rcppb.LogEntry, error) {
	panic("unimplemented")
}

// GetLogsFromIndex implements Database.
func (d *BoltDB) GetLogsFromIndex(index int64) ([]*rcppb.LogEntry, error) {
	panic("unimplemented")
}

// PrintAllLogs implements Database.
func (d *BoltDB) PrintAllLogs() error {
	panic("unimplemented")
}

// PrintAllLogsUnordered implements Database.
func (d *BoltDB) PrintAllLogsUnordered() error {
	panic("unimplemented")
}

// GetLastIndex implements Database.
func (d *BoltDB) GetLastIndexAndTerm() (int64, int64) {
	panic("unimplemented")
}

// Create Buckets
func (d *BoltDB) createBuckets() error {
	return d.DB.Update(func(tx *bolt.Tx) error {
		bucketsToCreate := [][]byte{
			constants.LogsBucket,
			constants.KvBucket,
			constants.Usertable,
		}

		for _, bucketName := range bucketsToCreate {
			if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", string(bucketName), err)
			}
		}
		return nil
	})
}

// func (d *BoltDB) insertLogsPersistent(appendEntryReq *rcppb.AppendEntriesReq) error {
// 	currIndex := appendEntryReq.PrevLogIndex + 1
// 	lastEntryTerm := appendEntryReq.Term
// 	return node.db.DB.Update(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)
// 		if b == nil {
// 			return errors.New("bucket not found 'logs'")
// 		}
// 		for _, entry := range appendEntryReq.Entries {
// 			// Lookup existing log at index
// 			key := fmt.Appendf(nil, "%d", currIndex)

// 			if _, ok := node.possibleFailureOrRecoveryIndex.Load(currIndex); ok {
// 				logBytes := b.Get(key)

// 				// Track removed nodes
// 				if logBytes != nil {
// 					var existingEntry rcppb.LogEntry
// 					err := json.Unmarshal(logBytes, &existingEntry)
// 					if err != nil {
// 						return fmt.Errorf("could not deserialize existing log at index %d: %v", currIndex, err)
// 					}

// 					if existingEntry.LogType == "failure" {
// 						node.removeFromFailureSet(existingEntry.NodeId)
// 					} else if existingEntry.LogType == "recovery" {
// 						node.removeFromRecoverySet(existingEntry.NodeId)
// 					}
// 				}
// 			}

// 			// Marshal and put new entry
// 			if entry.LogType == "failure" || entry.LogType == "success" {
// 				go node.possibleFailureOrRecoveryIndex.Store(currIndex, "")
// 			}
// 			newLogBytes, err := json.Marshal(entry)
// 			if err != nil {
// 				return fmt.Errorf("could not marshal new log entry at index %d: %v", currIndex, err)
// 			}
// 			err = b.Put(key, newLogBytes)
// 			if err != nil {
// 				return fmt.Errorf("failed to write log at index %d: %v", currIndex, err)
// 			}

// 			node.lastIndex = currIndex
// 			lastEntryTerm = entry.Term
// 			currIndex++
// 		}

// 		node.lastTerm = lastEntryTerm
// 		return nil
// 	})
// }

// // This method executes logs from node.execIndex + 1 to endIndex
// // This method is used in the persistent mode
// func (node *Node) persistentExecuteTill(endIndex int64) error {
// 	return node.db.DB.Update(func(tx *bolt.Tx) error {
// 		logsBkt := tx.Bucket(constants.LogsBucket)
// 		for node.execIndex < endIndex {
// 			logBytes := logsBkt.Get(fmt.Appendf(nil, "%d", node.execIndex+1))
// 			if logBytes == nil {
// 				return fmt.Errorf("executor() no log at index: %d", node.execIndex+1)
// 			}
// 			var logEntry rcppb.LogEntry
// 			err := json.Unmarshal(logBytes, &logEntry)
// 			if err != nil {
// 				return fmt.Errorf("could not deserialize logbytes of index %d into logentry: %v", node.execIndex+1, err)
// 			}

// 			switch logEntry.LogType {
// 			case "store":
// 				kvBkt := tx.Bucket([]byte(logEntry.Bucket))
// 				if kvBkt == nil {
// 					return fmt.Errorf("executor() index %d, no bucket %s", node.execIndex+1, logEntry.Bucket)
// 				}
// 				kvBkt.Put([]byte(logEntry.Key), []byte(logEntry.Value))
// 			case "delete":
// 				kvBkt := tx.Bucket([]byte(logEntry.Bucket))
// 				if kvBkt == nil {
// 					return fmt.Errorf("executor() index %d, no bucket %s", node.execIndex+1, logEntry.Bucket)
// 				}
// 				kvBkt.Delete([]byte(logEntry.Key))
// 			case "failure":
// 				node.currAlive -= 1
// 				node.serverStatusMap.Store(logEntry.NodeId, false)
// 				go node.removeFromFailureSet(logEntry.NodeId)
// 			case "recovery":
// 				node.currAlive += 1
// 				node.serverStatusMap.Store(logEntry.NodeId, true)
// 				go node.removeFromRecoverySet(logEntry.NodeId)
// 			}

// 			node.execIndex++
// 		}
// 		return nil
// 	})
// }

// func (d *BoltDB) GetLogAtIndex(index int64) (*rcppb.LogEntry, error) {
// 	var logBytes []byte
// 	d.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)
// 		logBytes = b.Get(fmt.Appendf(nil, "%d", index))
// 		return nil
// 	})
// 	if logBytes == nil {
// 		return &rcppb.LogEntry{}, fmt.Errorf("GetLogAtIndex(): no log at index %d", index)
// 	}

// 	var logEntry rcppb.LogEntry
// 	err := json.Unmarshal(logBytes, &logEntry)
// 	if err != nil {
// 		return &rcppb.LogEntry{}, fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
// 	}

// 	// log.Printf("Log at index %d: %v\n", index, &logEntry)
// 	return &logEntry, nil
// }

// // func (d *BoltDB) DeleteLogsStartingFromIndex(index int64) error {
// // 	return d.DB.Update(func(tx *bolt.Tx) error {
// // 		b := tx.Bucket(constants.LogsBucket)

// // 		for i := index; ; i++ {
// // 			key := fmt.Appendf(nil, "%d", i)
// // 			value := b.Get(key)
// // 			if value == nil {
// // 				break
// // 			}
// // 			log.Printf("Deleting key: %s, value: %s\n", string(key), string(value))
// // 			err := b.Delete(key)
// // 			if err != nil {
// // 				return fmt.Errorf("failed to delete index %s", key)
// // 			}
// // 			log.Printf("Deleted key %s\n", key)
// // 		}
// // 		return nil
// // 	})
// // }

// func (d *BoltDB) PrintAllLogs() error {
// 	return d.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)

// 		for i := 0; ; i++ {
// 			key := fmt.Appendf(nil, "%d", i)
// 			valueBytes := b.Get(key)
// 			if valueBytes == nil {
// 				break
// 			}

// 			var logEntry *rcppb.LogEntry
// 			err := json.Unmarshal(valueBytes, &logEntry)
// 			if err != nil {
// 				return fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
// 			}

// 			// log.Printf("%s %s\n", key, string(valueBytes))
// 			log.Printf("%d. Key: %s Bucket: %s\n", i+1, logEntry.Key, logEntry.Bucket)
// 		}
// 		return nil
// 	})
// }

// func (d *BoltDB) PrintAllLogsUnordered() error {
// 	return d.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)

// 		c := b.Cursor()
// 		for k, v := c.First(); k != nil; k, v = c.Next() {
// 			var logEntry *rcppb.LogEntry
// 			err := json.Unmarshal(v, &logEntry)
// 			if err != nil {
// 				return fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
// 			}

// 			key := string(k)
// 			fmt.Printf("Index: %s, Log Entry: %v\n", key, logEntry)
// 		}

// 		return nil
// 	})
// }

// func (d *BoltDB) GetLogsFromIndex(index int64) ([]*rcppb.LogEntry, error) {
// 	var logsSlice []*rcppb.LogEntry
// 	err := d.DB.Update(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)

// 		for i := index; ; i++ {
// 			logIndex := fmt.Sprintf("%d", i)
// 			logEntryBytes := b.Get([]byte(logIndex))
// 			if logEntryBytes == nil {
// 				break
// 			}
// 			logEntry := &rcppb.LogEntry{}
// 			err := json.Unmarshal(logEntryBytes, logEntry)
// 			if err != nil {
// 				log.Printf("Unable to unmarshal log index %d into LogEntry type\n", i)
// 				return err
// 			}
// 			logsSlice = append(logsSlice, logEntry)
// 		}
// 		return nil
// 	})
// 	return logsSlice, err
// }

// // func (d *BoltDB) PutLogAtIndex(index int64, logEntry *rcppb.LogEntry) (string, string, error) {
// // 	existingEntry, err := d.GetLogAtIndex(index)
// // 	failureNode := ""
// // 	recoveredNode := ""
// // 	if err == nil {
// // 		if existingEntry.LogType == "failure" {
// // 			failureNode = existingEntry.NodeId
// // 		} else if existingEntry.LogType == "recovery" {
// // 			recoveredNode = existingEntry.NodeId
// // 		}
// // 	}
// // 	// log.Printf("Putting %v at index %d\n", logEntry, index)
// // 	logBytes, err := json.Marshal(logEntry)
// // 	if err != nil {
// // 		return failureNode, recoveredNode, err
// // 	}
// // 	return failureNode, recoveredNode, d.DB.Update(func(tx *bolt.Tx) error {
// // 		b := tx.Bucket(constants.LogsBucket)
// // 		key := fmt.Appendf(nil, "%d", index)
// // 		return b.Put(key, logBytes)
// // 	})
// // }
