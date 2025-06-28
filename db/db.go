package db

import (
	"fmt"
	"log"
	"rcp/constants"

	bolt "go.etcd.io/bbolt"
)

var (
	
)

type Database struct {
	DB *bolt.DB
}


// Initialize boltDB
func InitDatabase(dbPath string) (db *Database, closeFunc func() error, err error) {
	log.Println("Initializing store")
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{
		DB: boltDB,
	}
	if err := db.createBuckets(); err != nil {
		boltDB.Close()
		return nil, nil, err
	}

	return db, boltDB.Close, nil
}


// Create Buckets
func (d *Database) createBuckets() error {
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
