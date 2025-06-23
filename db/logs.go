package db

import (
	"encoding/json"
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

func (d *Database) GetLogAtIndex(index int64) (*rcppb.LogEntry, error) {
	var logBytes []byte
	d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		logBytes = b.Get(fmt.Appendf(nil, "%d", index))
		return nil
	})
	if logBytes == nil {
		return &rcppb.LogEntry{}, fmt.Errorf("GetLogAtIndex(): no log at index %d", index)
	}

	var logEntry rcppb.LogEntry
	err := json.Unmarshal(logBytes, &logEntry)
	if err != nil {
		return &rcppb.LogEntry{}, fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
	}

	// log.Printf("Log at index %d: %v\n", index, &logEntry)
	return &logEntry, nil
}

// func (d *Database) DeleteLogsStartingFromIndex(index int64) error {
// 	return d.DB.Update(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)

// 		for i := index; ; i++ {
// 			key := fmt.Appendf(nil, "%d", i)
// 			value := b.Get(key)
// 			if value == nil {
// 				break
// 			}
// 			log.Printf("Deleting key: %s, value: %s\n", string(key), string(value))
// 			err := b.Delete(key)
// 			if err != nil {
// 				return fmt.Errorf("failed to delete index %s", key)
// 			}
// 			log.Printf("Deleted key %s\n", key)
// 		}
// 		return nil
// 	})
// }

func (d *Database) PrintAllLogs() error {
	return d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)

		for i := 0; ; i++ {
			key := fmt.Appendf(nil, "%d", i)
			valueBytes := b.Get(key)
			if valueBytes == nil {
				break
			}

			var logEntry *rcppb.LogEntry
			err := json.Unmarshal(valueBytes, &logEntry)
			if err != nil {
				return fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
			}

			// log.Printf("%s %s\n", key, string(valueBytes))
			log.Printf("%d. Key: %s Bucket: %s\n", i+1, logEntry.Key, logEntry.Bucket)
		}
		return nil
	})
}

func (d *Database) PrintAllLogsUnordered() error {
	return d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var logEntry *rcppb.LogEntry
			err := json.Unmarshal(v, &logEntry)
			if err != nil {
				return fmt.Errorf("could not deserialize logbytes into logentry: %v", err)
			}

			key := string(k)
			fmt.Printf("Index: %s, Log Entry: %v\n", key, logEntry)
		}

		return nil
	})
}

func (d *Database) GetLogsFromIndex(index int64) ([]*rcppb.LogEntry, error) {
	var logsSlice []*rcppb.LogEntry
	err := d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)

		for i := index; ; i++ {
			logIndex := fmt.Sprintf("%d", i)
			logEntryBytes := b.Get([]byte(logIndex))
			if logEntryBytes == nil {
				break
			}
			logEntry := &rcppb.LogEntry{}
			err := json.Unmarshal(logEntryBytes, logEntry)
			if err != nil {
				log.Printf("Unable to unmarshal log index %d into LogEntry type\n", i)
				return err
			}
			logsSlice = append(logsSlice, logEntry)
		}
		return nil
	})
	return logsSlice, err
}

// func (d *Database) PutLogAtIndex(index int64, logEntry *rcppb.LogEntry) (string, string, error) {
// 	existingEntry, err := d.GetLogAtIndex(index)
// 	failureNode := ""
// 	recoveredNode := ""
// 	if err == nil {
// 		if existingEntry.LogType == "failure" {
// 			failureNode = existingEntry.NodeId
// 		} else if existingEntry.LogType == "recovery" {
// 			recoveredNode = existingEntry.NodeId
// 		}
// 	}
// 	// log.Printf("Putting %v at index %d\n", logEntry, index)
// 	logBytes, err := json.Marshal(logEntry)
// 	if err != nil {
// 		return failureNode, recoveredNode, err
// 	}
// 	return failureNode, recoveredNode, d.DB.Update(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(constants.LogsBucket)
// 		key := fmt.Appendf(nil, "%d", index)
// 		return b.Put(key, logBytes)
// 	})
// }
