package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"rcp/constants"
	"rcp/grpc/orcapb"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
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

func logKey(index int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(index))
	return buf
}

func logIndexFromKey(key []byte) int64 {
	if len(key) != 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(key))
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
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return ErrNotFound
		}
		valueBytes := b.Get([]byte(key))
		if valueBytes == nil {
			return ErrNotFound
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
	return d.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("bucket %s create: %w", bucket, err)
		}
		return b.Put([]byte(key), []byte(value))
	})
}

// Delete implements Database.
func (d *BoltDB) Delete(key string, bucket string) error {
	return d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return ErrNotFound
		}
		return b.Delete([]byte(key))
	})
}

// AppendLog implements Database.
func (d *BoltDB) AppendLog(logEntry *orcapb.LogEntry) (int64, error) {
	var index int64
	err := d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		lastKey, _ := cursor.Last()
		if lastKey == nil {
			index = 0
		} else {
			index = logIndexFromKey(lastKey) + 1
		}
		data, err := proto.Marshal(logEntry)
		if err != nil {
			return err
		}
		return b.Put(logKey(index), data)
	})
	return index, err
}

// PutLogAtIndex implements Database.
func (d *BoltDB) PutLogAtIndex(index int64, log *orcapb.LogEntry) error {
	if index < 0 {
		return ErrNotFound
	}
	return d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		lastKey, _ := cursor.Last()
		lastIndex := int64(-1)
		if lastKey != nil {
			lastIndex = logIndexFromKey(lastKey)
		}

		if index > lastIndex+1 {
			return ErrSkippedIndex
		}

		data, err := proto.Marshal(log)
		if err != nil {
			return err
		}
		return b.Put(logKey(index), data)
	})
}

// GetLogAtIndex implements Database.
func (d *BoltDB) GetLogAtIndex(index int64) (*orcapb.LogEntry, error) {
	if index < 0 {
		return nil, ErrNotFound
	}
	var entry *orcapb.LogEntry
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		data := b.Get(logKey(index))
		if data == nil {
			return ErrNotFound
		}
		entry = &orcapb.LogEntry{}
		return proto.Unmarshal(data, entry)
	})
	return entry, err
}

// GetLogsFromIndex implements Database.
func (d *BoltDB) GetLogsFromIndex(index int64, maxLogs int) ([]*orcapb.LogEntry, error) {
	if maxLogs <= 0 {
		return []*orcapb.LogEntry{}, nil
	}
	if index < 0 {
		index = 0
	}
	entries := make([]*orcapb.LogEntry, 0, maxLogs)
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		for k, v := cursor.Seek(logKey(index)); k != nil && len(entries) < maxLogs; k, v = cursor.Next() {
			entry := &orcapb.LogEntry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// TruncateFrom implements Database.
func (d *BoltDB) TruncateFrom(index int64) error {
	if index <= 0 {
		index = 0
	}
	return d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		k, _ := cursor.Seek(logKey(index))
		if k == nil {
			return ErrNotFound
		}
		for ; k != nil; k, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// PrintAllLogs implements Database.
func (d *BoltDB) PrintAllLogs() error {
	return d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			entry := &orcapb.LogEntry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				return err
			}
			log.Printf("log[%d]=%v", logIndexFromKey(k), entry)
		}
		return nil
	})
}

// PrintAllLogsUnordered implements Database.
func (d *BoltDB) PrintAllLogsUnordered() error {
	return d.PrintAllLogs()
}

// GetLastIndex implements Database.
func (d *BoltDB) GetLastIndex() (int64, error) {
	var lastIndex int64 = -1
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		k, _ := cursor.Last()
		if k == nil {
			lastIndex = -1
			return nil
		}
		lastIndex = logIndexFromKey(k)
		return nil
	})
	return lastIndex, err
}

// GetLastTerm implements Database.
func (d *BoltDB) GetLastTerm() (int64, error) {
	var lastTerm int64
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(constants.LogsBucket)
		if b == nil {
			return errors.New("logs bucket not found")
		}
		cursor := b.Cursor()
		_, v := cursor.Last()
		if v == nil {
			lastTerm = 0
			return nil
		}
		entry := &orcapb.LogEntry{}
		if err := proto.Unmarshal(v, entry); err != nil {
			return err
		}
		lastTerm = entry.Term
		return nil
	})
	return lastTerm, err
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
