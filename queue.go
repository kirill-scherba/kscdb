package kscdb

import (
	"time"

	"github.com/gocql/gocql"
)

// Queue define Named Queue Database methods
type Queue struct {
	*Kscdb
}

// SetQueue add value to named queue by key (name of queue)
func (q *Kscdb) SetQueue(key string, value []byte) (err error) {
	return q.session.Query(
		`UPDATE queue SET lock = '', data = ? WHERE key = ? AND time = toTimestamp(now()) AND random = UUID()`,
		value, key).Exec()
}

// GetQueue get first value from named queue by key (name of queue)
func (q *Kscdb) GetQueue(key string) (data []byte, err error) {
	// Get free value
	var time time.Time
	var random string
	if err = q.session.Query(
		`SELECT time, random, data FROM queue WHERE key = ? AND lock = '' LIMIT 1 ALLOW FILTERING`,
		key).Consistency(gocql.One).Scan(&time, &random, &data); err != nil {
		return
	}

	// Loc record (to allow concurency)
	var ok bool
	var lock string
	if err = q.session.Query(
		`UPDATE queue SET lock = 'locked' WHERE key = ? AND time = ? AND random = ? IF lock = ''`,
		key, time, random).Consistency(gocql.One).Scan(&ok, &lock); err != nil {
		return
	}
	if !ok {
		return q.GetQueue(key)
	}

	// Delete locket record from queue and return value
	err = q.session.Query(
		`DELETE FROM queue WHERE key = ? AND time = ? AND random = ?`,
		key, time, random).Exec()
	return
}
