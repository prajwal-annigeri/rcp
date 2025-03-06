package db

import (
	"encoding/json"
	"fmt"
	"log"
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
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)
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

	log.Printf("Log at index %d: %v\n", index, &logEntry)

	return &logEntry, nil
}

func (d *Database) DeleteLogsStartingFromIndex(index int64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)

		for i := index; ; i++ {
			key := fmt.Appendf(nil, "%d", i)
			value := b.Get(key)
			if value == nil {
				break
			}
			log.Printf("Deleting key: %s, value: %s\n", string(key), string(value))
			err := b.Delete(key)
			if err != nil {
				return fmt.Errorf("failed to delete index %s", key)
			}
			log.Printf("Deleted key %s\n", key)
		}
		return nil
	})
}

func (d *Database) InsertLogs(req *rcppb.AppendEntriesReq) error {
	currIndex := req.PrevLogIndex + 1
	var err error
	for _, entry := range req.Entries {
		log.Printf("Putting entry: %v\n", entry)
		err = d.PutLogAtIndex(currIndex, entry)
		if err != nil {
			return err
		}
		currIndex += 1
	}
	return nil
}

func (d *Database) PrintAllLogs() error {
	return d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)

		for i := 0; ; i++ {
			key := fmt.Appendf(nil, "%d", i)
			valueBytes := b.Get(key)
			if valueBytes == nil {
				break
			}

			log.Printf("%d. %s %s\n", i, key, string(valueBytes))
		}
		return nil
	})
}

func (d *Database) PrintAllLogsUnordered() error {
	return d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)

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
	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)

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


func (d *Database) PutLogAtIndex(index int64, logEntry *rcppb.LogEntry) error {
	log.Printf("Putting %v at index %d\n", logEntry, index)
	logBytes, err := json.Marshal(logEntry)
	if err != nil {
		return err
	}
	return d.db.Update(func (tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)
		key := fmt.Appendf(nil, "%d", index)
		return b.Put(key, logBytes)
	})
}
