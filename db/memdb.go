package db

import (
	"fmt"
	"rcp/rcppb"
)

type MemDB struct {
	logs []*rcppb.LogEntry
	kv   map[string]string
}

func InitMemoryDatabase() (db *MemDB) {
	return &MemDB{
		logs: []*rcppb.LogEntry{},
		kv:   make(map[string]string),
	}
}

// Get implements Database.
func (d *MemDB) Get(key string, bucket string) (string, error) {
	val, ok := d.kv[fmt.Sprintf("%s/%s", bucket, key)]
	if !ok {
		err := fmt.Errorf("no value for key: %s in bucket %s", key, bucket)
		return "", err
	}
	return val, nil
}

// Store implements Database.
func (d *MemDB) Store(key string, bucket string, value string) error {
	d.kv[fmt.Sprintf("%s/%s", bucket, key)] = value
	return nil
}

// Delete implements Database.
func (d *MemDB) Delete(key string, bucket string) error {
	delete(d.kv, fmt.Sprintf("%s/%s", bucket, key))
	return nil
}

// PutLogAtIndex implements Database.
func (d *MemDB) PutLogAtIndex(index int64, log *rcppb.LogEntry) error {
	if len(d.logs) > int(index) {
		return ErrAlreadyExists
	}

	if len(d.logs) < int(index) {
		return ErrSkippedIndex
	}

	d.logs = append(d.logs, log)
	return nil
}

// GetLogAtIndex implements Database.
func (d *MemDB) GetLogAtIndex(index int64) (*rcppb.LogEntry, error) {
	if len(d.logs) < int(index) {
		return nil, ErrNotFound
	}
	return d.logs[index], nil
}

// GetLogsFromIndex implements Database.
// Returns empty array if no logs from and after index
func (d *MemDB) GetLogsFromIndex(index int64) ([]*rcppb.LogEntry, error) {
	if len(d.logs) < int(index) {
		return []*rcppb.LogEntry{}, nil
	}
	return d.logs[index:], nil
}

// PrintAllLogs implements Database.
func (d *MemDB) PrintAllLogs() error {
	panic("unimplemented")
	// 	for i := int64(0); i <= node.lastIndex; i++ {
	// 		logEntryRaw, ok := d.logs.Load(i)
	// 		if !ok {
	// 			log.Printf("No log at index %d", i)
	// 			continue
	// 		}

	// 		logEntry := logEntryRaw.(*rcppb.LogEntry)
	// 		if logEntry.Key != "" {
	// 			log.Printf("%d. Key: %s Bucket: %s\n", i+1, logEntry.Key, logEntry.Bucket)
	// 		} else {
	// 			log.Printf("%d. %s %s", i+1, logEntry.LogType, logEntry.NodeId)
	// 		}

	// }
}

// PrintAllLogsUnordered implements Database.
func (d *MemDB) PrintAllLogsUnordered() error {
	panic("unimplemented")
}

// GetLastIndexAndTerm implements Database.
func (d *MemDB) GetLastIndexAndTerm() (int64, int64) {
	if len(d.logs) == 0 {
		return -1, -1
	}

	return int64(len(d.logs)), d.logs[len(d.logs)-1].GetTerm()
}
