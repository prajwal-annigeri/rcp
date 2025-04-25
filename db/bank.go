package db

import (
	"context"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

type AccountType string

const (
	CheckingAccount AccountType = "checking"
	SavingsAccount  AccountType = "savings"
)

func (d *Database) getBucketByType(tx *bolt.Tx, accountType AccountType) (*bolt.Bucket, error) {
	var bucketName []byte
	switch accountType {
	case CheckingAccount:
		bucketName = checkingBucket
	case SavingsAccount:
		bucketName = savingsBucket
	default:
		return nil, fmt.Errorf("invalid account type: %s", accountType)
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		// This should not happen if createBuckets ran successfully
		return nil, fmt.Errorf("bucket %s not found", string(bucketName))
	}
	return b, nil
}

func (d *Database) GetBalance(accountID string, accountType AccountType) (int64, error) {
	var balance int64
	keyBytes := []byte(accountID)

	err := d.db.View(func(tx *bolt.Tx) error {
		b, err := d.getBucketByType(tx, accountType)
		if err != nil {
			return err
		}

		valueBytes := b.Get(keyBytes)
		if valueBytes == nil {
			return fmt.Errorf("no entry for account id: %s (%s)", accountID, accountType)
		}

		// Convert byte slice back to string, then parse string to uint64
		balanceString := string(valueBytes)
		parsedBalance, err := strconv.ParseInt(balanceString, 10, 64)
		if err != nil {
			return fmt.Errorf("corrupt balance data for account %s (%s): failed to parse '%s' as int64: %w",
				accountID, accountType, balanceString, err)
		}

		balance = parsedBalance
		return nil
	})

	if err != nil {
		return 0, err
	}
	return balance, nil
}


func (d *Database) PutBalance(accountID string, accountType AccountType, balance int64) error {
	keyBytes := []byte(accountID)

	// Convert balance int64 to its base-10 string representation, then to bytes
	balanceString := strconv.FormatInt(balance, 10)
	valueBytes := []byte(balanceString)

	return d.db.Update(func(tx *bolt.Tx) error {
		b, err := d.getBucketByType(tx, accountType)
		if err != nil {
			return err
		}

		if err := b.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to put balance for account %s (%s): %w",
				accountID, accountType, err)
		}
		return nil
	})
}

func (d *Database) ModifyBalance(accountID string, accountType AccountType, amount int64) error {
	err := d.Lock(context.Background(), accountID)
	defer d.Unlock(accountID)
	if err != nil {
		return err
	}
	keyBytes := []byte(accountID)

	return d.db.Update(func(tx *bolt.Tx) error {
		b, err := d.getBucketByType(tx, accountType)
		if err != nil {
			return err
		}

		// Retrieve the current balance
		valueBytes := b.Get(keyBytes)
		if valueBytes == nil {
			return fmt.Errorf("no entry for account id: %s (%s)", accountID, accountType)
		}

		balanceString := string(valueBytes)
		currentBalance, err := strconv.ParseInt(balanceString, 10, 64)
		if err != nil {
			return fmt.Errorf("corrupt balance data for account %s (%s): failed to parse '%s' as int64: %w",
				accountID, accountType, balanceString, err)
		}

		newBalance := currentBalance + amount

		// Store the new balance within the same transaction
		balanceString = strconv.FormatInt(newBalance, 10)
		valueBytes = []byte(balanceString)

		if err := b.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to put new balance for account %s (%s): %w",
				accountID, accountType, err)
		}

		return nil
	})
}
