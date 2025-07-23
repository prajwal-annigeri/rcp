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
	PutLogAtIndex(index int64, log *rcppb.LogEntry) error
	GetLogAtIndex(index int64) (*rcppb.LogEntry, error)
	GetLogsFromIndex(index int64) ([]*rcppb.LogEntry, error)
	PrintAllLogs() error
	PrintAllLogsUnordered() error

	GetLastIndexAndTerm() (int64, int64)
}
