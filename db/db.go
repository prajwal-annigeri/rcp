package db

import "rcp/grpc/orcapb"

// KVStore defines the key/value operations that power clients and tests.
type KVStore interface {
	Get(key string, bucket string) (string, error)
	Store(key string, bucket string, value string) error
	Delete(key string, bucket string) error
}

// LogStore captures the log-related operations used by the consensus protocol.
type LogStore interface {
	AppendLog(log *orcapb.LogEntry) (int64, error)
	PutLogAtIndex(index int64, log *orcapb.LogEntry) error
	GetLogAtIndex(index int64) (*orcapb.LogEntry, error)
	GetLogsFromIndex(index int64, maxLogs int) ([]*orcapb.LogEntry, error)
	PrintAllLogs() error
	PrintAllLogsUnordered() error
	GetLastIndex() (int64, error)
	GetLastTerm() (int64, error)
}

// Database currently exposes both the KV and log concerns.
type Database interface {
	KVStore
	LogStore
}
