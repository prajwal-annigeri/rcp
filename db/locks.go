package db

import (
	"context"
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	// defaultLockPollInterval defines how long to wait between checking if a lock is free.
	defaultLockPollInterval = 10 * time.Millisecond
	// lockValue is a simple placeholder written to the locks bucket when a lock is acquired.
	// Using a non-empty slice can sometimes be preferable to an empty one.
	lockValue = byte(1)
)

// Lock attempts to acquire an exclusive lock for the given accountID.
// If the lock is already held (key exists in locksBucket), it waits and polls
// periodically until the lock is released or the context is cancelled.
// Once free, it acquires the lock by inserting the accountID into the locksBucket.
// Returns nil on successful lock acquisition, error otherwise (e.g., context timeout/cancel, db error).
func (d *Database) Lock(ctx context.Context, accountID string) error {
	keyBytes := []byte(accountID)
	pollInterval := defaultLockPollInterval // Could be made configurable
	
	for {
		// Check for context cancellation before potentially blocking operations
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock acquisition cancelled or timed out for account %s: %w", accountID, ctx.Err())
		default:
			// Continue if context is still active
		}

		// Attempt to acquire the lock within a single atomic transaction
		var acquired bool
		err := d.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(locksBucket)
			if b == nil {
				return fmt.Errorf("locks bucket not found during lock attempt")
			}

			// CRITICAL: Check if lock exists *inside* the write transaction
			existingValue := b.Get(keyBytes)
			if existingValue != nil {
				// Lock is currently held by someone else.
				// Signal that we didn't acquire it and exit the transaction cleanly.
				acquired = false
				return nil
			}

			// Lock is free, acquire it by putting the key.
			// Use a simple placeholder value.
			if err := b.Put(keyBytes, []byte{lockValue}); err != nil {
				// Error occurred trying to write the lock
				return fmt.Errorf("failed to put lock key for account %s: %w", accountID, err)
			}

			// Signal that we successfully acquired the lock
			acquired = true
			return nil
		})

		// Handle errors that occurred during the d.db.Update process itself
		if err != nil {
			return fmt.Errorf("database error during lock acquisition attempt for account %s: %w", accountID, err)
		}

		// If we successfully acquired the lock in the transaction, return success
		if acquired {
			log.Printf("Locked %s", accountID)
			return nil // Lock acquired!
		}

		// If lock was not acquired (it was already held when checked inside Update),
		// wait for the poll interval before trying again.
		// Use a timer that respects context cancellation.
		select {
		case <-time.After(pollInterval):
			// Interval elapsed, loop will continue and try again
		case <-ctx.Done():
			// Context cancelled while waiting
			return fmt.Errorf("lock acquisition cancelled or timed out while polling for account %s: %w", accountID, ctx.Err())
		}
	}
}

// Unlock releases the exclusive lock for the given accountID.
// It does this by removing the accountID key from the locksBucket.
// This operation is idempotent - calling Unlock on a non-locked accountID
// will not return an error (unless a database error occurs).
func (d *Database) Unlock(accountID string) error {
	keyBytes := []byte(accountID)
	log.Printf("Unlocking %s", accountID)
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(locksBucket)

		// Delete the key associated with the account ID.
		// BoltDB's Delete does not return an error if the key doesn't exist.
		if err := b.Delete(keyBytes); err != nil {
			log.Printf("failed to delete lock key for account %s: %v", accountID, err)
			return fmt.Errorf("failed to delete lock key for account %s: %w", accountID, err)
		}
		return nil // Success
	})
}
