package kscdb

import (
	"fmt"
	"log"
	"strconv"

	"github.com/gocql/gocql"
)

// IDS define digital ID methods
type IDs struct {
	*Kscdb
}

// SetID set keys next ID value
func (ids *IDs) Set(key string, value []byte) (err error) {
	nextID, err := strconv.Atoi(string(value))
	if err != nil {
		return
	}
	// return cdb.session.Query(`UPDATE ids SET next_id = ? WHERE id_name = ?`,
	// 	nextID, key).Exec()
	return ids.set(key, nextID)
}

// GetID returns new diginal ID for key, ID just increments
func (ids *IDs) Get(key string) (data []byte, err error) {

	return ids.getAws(key)

	// if 1 != 1 {
	// 	// var nextID int
	// 	// Read current counter value with id_name
	// 	nextID, err := ids.get(key)
	// 	if err != nil {
	// 		return
	// 	}

	// 	// Set new next_id and return current next_id
	// 	var ok bool
	// 	// ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	// 	// defer cancel()
	// 	for {
	// 		// Increment nextID in database
	// 		if err = ids.session.Query(
	// 			`UPDATE ids SET next_id = ? WHERE id_name = ? IF next_id = ?`,
	// 			nextID+1, key, nextID). /* .WithContext(ctx) */ Scan(&ok, &nextID); err != nil {
	// 			log.Println("Increment current counter error:", err)
	// 			// continue
	// 			return
	// 		}
	// 		log.Println("Update result:", ok, nextID)
	// 		if ok {
	// 			break
	// 		}
	// 	}

	// 	// Return received nextID in text
	// 	data = []byte(fmt.Sprintf("%d", nextID))
	// }
	// return
}

// Get ID for AWS Keyspaces
func (ids *IDs) getAws(key string) (data []byte, err error) {

	// Lock ID table
	lockKey := key + "/lock"
	lockid, err := ids.Lock(lockKey)
	if err != nil {
		log.Println("Loc error:", lockKey, err)
		return
	}
	defer ids.Unlock(lockKey, lockid)

	// Read bext ID
	nextID, err := ids.get(key)
	if err != nil {
		return
	}

	// Save new nextID
	err = ids.set(key, nextID+1)
	if err != nil {
		return
	}

	// Return received nextID in text
	data = []byte(fmt.Sprintf("%d", nextID))

	return
}

// get new diginal ID for key, ID just increments
func (ids *IDs) get(key string) (nextID int, err error) {
	// Read current counter value with id_name
	if err = ids.session.Query(`SELECT next_id FROM ids WHERE id_name = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&nextID); err != nil {

		// Check error
		if err != gocql.ErrNotFound {
			log.Println("Read current counter error:", err)
			return
		}

		// Create new record if counter with id_name does not exists
		nextID = 1
		if err = ids.Set(key, []byte(strconv.Itoa(nextID))); err != nil {
			return
		}
	}

	return
}

// set keys next ID value
func (ids *IDs) set(key string, nextID int) (err error) {
	if err = ids.session.Query(
		`UPDATE ids SET next_id = ? WHERE id_name = ?`,
		nextID, key).Exec(); err != nil {
		log.Println("Set current counter error:", err)
		return
	}
	return
}

// Delete counter from database by key
func (ids *IDs) Delete(key string) (err error) {
	return ids.session.Query(`DELETE FROM ids WHERE id_name = ?`,
		key).Exec()
}
