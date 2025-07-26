package db

import (
	"rcp/rcppb"
)

type Database interface {
	// Key value store related
	Get(key string, bucket string) (string, error)
	Store(key string, bucket string, value string) error
	Delete(key string, bucket string) error

	// Logs related
	AppendLog(log *rcppb.LogEntry) (int64, error)
	PutLogAtIndex(index int64, log *rcppb.LogEntry) error
	GetLogAtIndex(index int64) (*rcppb.LogEntry, error)
	GetLogsFromIndex(index int64, maxLogs int) ([]*rcppb.LogEntry, error)
	PrintAllLogs() error
	PrintAllLogsUnordered() error

	GetLastIndex() (int64, error)
	GetLastTerm() (int64, error)
}
