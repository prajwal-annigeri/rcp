package db

import (
	"fmt"
	"rcp/grpc/orcapb"
)

type MemDB struct {
	logs []*orcapb.LogEntry
	kv   map[string]string
}

func InitMemoryDatabase() (db *MemDB) {
	return &MemDB{
		logs: []*orcapb.LogEntry{},
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

// AppendLog implements Database.
func (d *MemDB) AppendLog(log *orcapb.LogEntry) (int64, error) {
	d.logs = append(d.logs, log)
	return int64(len(d.logs) - 1), nil
}

// PutLogAtIndex implements Database.
func (d *MemDB) PutLogAtIndex(index int64, log *orcapb.LogEntry) error {
	if len(d.logs) > int(index) {
		// Log already exists
		d.logs[index] = log
		return nil
	}

	if len(d.logs) < int(index) {
		return ErrSkippedIndex
	}

	d.logs = append(d.logs, log)
	return nil
}

// GetLogAtIndex implements Database.
func (d *MemDB) GetLogAtIndex(index int64) (*orcapb.LogEntry, error) {
	if len(d.logs) <= int(index) {
		return nil, ErrNotFound
	}
	return d.logs[index], nil
}

// GetLogsFromIndex implements Database.
// Returns empty array if no logs from and after index
func (d *MemDB) GetLogsFromIndex(index int64, maxLogs int) ([]*orcapb.LogEntry, error) {
	if len(d.logs) < int(index) {
		return []*orcapb.LogEntry{}, nil
	}

	end := min(len(d.logs), int(index)+maxLogs)
	return d.logs[index:end], nil
}

// TruncateFrom removes log entries starting at index.
func (d *MemDB) TruncateFrom(index int64) error {
	if index <= 0 {
		d.logs = d.logs[:0]
		return nil
	}
	if int(index) > len(d.logs) {
		return ErrNotFound
	}
	d.logs = d.logs[:index]
	return nil
}

// PrintAllLogs implements Database.
func (d *MemDB) PrintAllLogs() error {
	panic("unimplemented")
}

// PrintAllLogsUnordered implements Database.
func (d *MemDB) PrintAllLogsUnordered() error {
	panic("unimplemented")
}

// GetLastIndex implements Database.
func (d *MemDB) GetLastIndex() (int64, error) {
	if len(d.logs) == 0 {
		return -1, nil
	}

	return int64(len(d.logs) - 1), nil
}

// GetLastTerm implements Database.
func (d *MemDB) GetLastTerm() (int64, error) {
	if len(d.logs) == 0 {
		// Raft only allows term >= 0
		return 0, nil
	}

	return d.logs[len(d.logs)-1].GetTerm(), nil
}
