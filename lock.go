package kscdb

import (
	"errors"
	"log"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

// Lock access for save concurrence
func (cdb *Kscdb) Lock(key string) (lockid string, err error) {

	// Create UUID
	lockid = uuid.New().String()

	// ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	// defer cancel()
	for {
		// Seve UUID to keyvalue
		err = cdb.session.Query(
			// `UPDATE map SET data = ? WHERE key = ? IF NOT EXIST`,
			`INSERT INTO map (key, data) VALUES (?,?) IF NOT EXISTS`,
			key, []byte(lockid)). /* .WithContext(ctx) */ Exec()
		if err != nil {
			log.Println("Lock INSERT err:", key, lockid, err)
			// continue
			// return
		}

		// Check if UUID saved
		var data []byte
		data, err = cdb.Map.Get(key)
		if err != nil {
			if err == gocql.ErrNotFound {
				// log.Println("Lock Get next", key, err)
				continue
			}
			log.Println("Lock Get err:", key, err)
			return
		}
		// log.Println("check lock saved", key, string(data), err)
		if string(data) == lockid {
			break
		}
	}

	return
}

// Unlock access for save concurrence
func (cdb *Kscdb) Unlock(key string, lockids ...string) (err error) {

	// Get Lock value by key
	data, err := cdb.Map.Get(key)
	if err != nil {
		return
	}

	// Check lockid if second function parameter is defined
	if len(lockids) > 0 && string(data) != lockids[0] {
		err = errors.New("can't unlock, the lockid not equal")
		return
	}

	// Delete lock key
	err = cdb.Map.Delete(key)

	return
}
