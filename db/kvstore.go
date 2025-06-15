package db

import (
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

// func (d *Database) PutKV(key, value, bucketName string) error {
// 	log.Printf("Storing key %s to bucket %s\n", key, bucketName)
// 	return d.DB.Batch(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		if b == nil {
// 			log.Printf("Bucket %s does not exist", bucketName)
// 			return fmt.Errorf("bucket %s does not exist", bucketName)
// 		}
// 		return b.Put([]byte(key), []byte(value))
// 	})
// }

func (d *Database) GetKV(key, bucketName string) (string, error) {
	var value string
	log.Printf("Getting key '%s' from bucket '%s'", key, bucketName)
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			log.Printf("Bucket %s does not exist", bucketName)
			return fmt.Errorf("bucket %s does not exist", bucketName)
		}
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

// func (d *Database) DeleteKV(key, bucketName string) error {
// 	log.Printf("Deleting key: %s bucket: %s", key, bucketName)
// 	return d.DB.Batch(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		if b == nil {
// 			return fmt.Errorf("bucket %s does not exist", bucketName)
// 		}
// 		err := b.Delete([]byte(key))
// 		if err != nil {
// 			return fmt.Errorf("error deleting key %s from bucket %s: %v", key, bucketName, err)
// 		}
// 		return nil
// 	})
// }
