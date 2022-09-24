package kscdb

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

const queueTable = "queue2"

// Queue define Named Queue Database methods
type Queue struct {
	*Kscdb
}

var ErrNotFound = gocql.ErrNotFound

// Set add value to named queue by key (name of queue)
func (q *Queue) Set(key string, value []byte) (err error) {
	uuid := uuid.New().String()
	return q.session.Query(
		`UPDATE `+queueTable+` SET lock = '', data = ? WHERE key = ? AND time = toTimestamp(now()) AND random = ?`,
		value, key, uuid).Exec()
}

// Get get first value from named queue by key (name of queue)
func (q *Queue) Get(key string) (data []byte, err error) {

	// if q.aws {
	return q.getAWS(key)
	//}

	// Get free value
	var time time.Time
	var random string
	if err = q.session.Query(
		`SELECT time, random, data FROM `+queueTable+` WHERE key = ? AND lock = '' LIMIT 1 ALLOW FILTERING`,
		key).Consistency(gocql.One).Scan(&time, &random, &data); err != nil {
		return
	}

	// Loc record (to allow concurency)
	var ok bool
	var lock string
	if err = q.session.Query(
		`UPDATE `+queueTable+` SET lock = 'locked' WHERE key = ? AND time = ? AND random = ? IF lock = ''`,
		key, time, random).Consistency(gocql.One).Scan(&ok, &lock); err != nil {
		return
	}
	if !ok {
		return q.Get(key)
	}

	// Delete locket record from queue and return value
	err = q.session.Query(
		`DELETE FROM `+queueTable+` WHERE key = ? AND time = ? AND random = ?`,
		key, time, random).Exec()
	return
}

// getAWS get first value from named queue by key (name of queue)
func (q *Queue) getAWS(key string) (data []byte, err error) {

	// Lock by key
	lockid, err := q.Lock(key)
	if err != nil {
		return
	}
	defer q.Unlock(key, lockid)

	// Get first value
	// log.Println("Get")
	var saveTime time.Time
	var random string
	if err = q.session.Query(
		`SELECT time, random, data FROM `+queueTable+` WHERE key = ? AND lock = '' LIMIT 1 ALLOW FILTERING`,
		key).Consistency(gocql.One).Scan(&saveTime, &random, &data); err != nil {
		return
	}

	// log.Println("Get", time, random, string(data))

	// Delete locket record from queue and return value
	err = q.session.Query(
		`DELETE FROM `+queueTable+` WHERE key = ? AND time = ? AND random = ?`,
		key, saveTime, random).Exec()

	// time.Sleep(30*time.Millisecond)

	return
}

// Clear remove all records from named queue by key
func (q *Queue) Clear(key string) (data []byte, err error) {
	err = q.session.Query(`DELETE FROM `+queueTable+` WHERE key = ?`,key).Exec()
	return
}