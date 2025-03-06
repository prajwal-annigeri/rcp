package db

import (
	bolt "go.etcd.io/bbolt"
)

var (
	logsBucket = []byte("logs")
	kvBucket   = []byte("store")
)

type Database struct {
	db *bolt.DB
}


func InitDatabase(dbPath string) (db *Database, closeFunc func() error, err error) {
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{
		db: boltDB,
	}
	if err := db.createBuckets(); err != nil {
		boltDB.Close()
		return nil, nil, err
	}

	return db, boltDB.Close, nil
}

func (d *Database) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logsBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(kvBucket); err != nil {
			return err
		}

		return nil
	})
}
