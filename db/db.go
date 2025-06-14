package db

import (
	"fmt"
	"rcp/constants"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

var (
	
)

type Database struct {
	DB *bolt.DB
}

func InitDatabase(dbPath string) (db *Database, closeFunc func() error, err error) {
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

	if err := db.initializeAccounts(); err != nil {
		boltDB.Close()
		return nil, nil, err
	}

	return db, boltDB.Close, nil
}

func (d *Database) createBuckets() error {
	return d.DB.Update(func(tx *bolt.Tx) error {
		bucketsToCreate := [][]byte{
			constants.LogsBucket,
			constants.KvBucket,
			constants.SavingsBucket,
			constants.CheckingBucket,
			constants.LocksBucket,
			constants.PendingSavings,
			constants.PendingChecking,
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

func (d *Database) initializeAccounts() error {
	balanceBytes := []byte("100")

	return d.DB.Update(func(tx *bolt.Tx) error {
		checkingB := tx.Bucket(constants.CheckingBucket)
		if checkingB == nil {
			return fmt.Errorf("checking bucket not found during init")
		}
		savingsB := tx.Bucket(constants.SavingsBucket)
		if savingsB == nil {
			return fmt.Errorf("savings bucket not found during init")
		}

		pendingSavingsBkt := tx.Bucket(constants.PendingSavings)
		if pendingSavingsBkt == nil {
			return fmt.Errorf("pending savings bucket not found during init")
		}

		pendingCheckingBkt := tx.Bucket(constants.PendingChecking)
		if pendingCheckingBkt == nil {
			return fmt.Errorf("pending checking bucket not found during init")
		}

		for i := int64(1); i <= 100000; i++ {
			keyBytes := []byte(strconv.FormatInt(i, 10))
			if err := checkingB.Put(keyBytes, balanceBytes); err != nil {
				return fmt.Errorf("failed to set checking balance for account %d: %w", i, err)
			}
			if err := savingsB.Put(keyBytes, balanceBytes); err != nil {
				return fmt.Errorf("failed to set savings balance for account %d: %w", i, err)
			}
			if err := pendingSavingsBkt.Put(keyBytes, balanceBytes); err != nil {
				return fmt.Errorf("failed to set pending savings balance for account %d: %w", i, err)
			}

			if err := pendingCheckingBkt.Put(keyBytes, balanceBytes); err != nil {
				return fmt.Errorf("failed to set pending checking balance for account %d: %w", i, err)
			}
		}
		return nil
	})
}
