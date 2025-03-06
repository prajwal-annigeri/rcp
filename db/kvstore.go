package db

import (
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

func (d *Database) PutKV(key, value string) error {
	log.Printf("Stroing %s: %s\n", key, value)
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)

		return b.Put([]byte(key), []byte(value))
	})
}

func (d *Database) GetKV(key string) (string, error) {
	var value string
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)
		valueBytes := b.Get([]byte(key))
		if valueBytes == nil {
			return fmt.Errorf("no value for key %s", key)
		}
		value = string(valueBytes)
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}
