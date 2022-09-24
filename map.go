package kscdb

import (
	"github.com/gocql/gocql"
)

// Map define KeyValue Database methods
type Map struct {
	*Kscdb
}

// Set key value
func (m *Map) Set(key string, value []byte) (err error) {
	err = m.session.Query(`UPDATE map SET data = ? WHERE key = ?`,
		value, key).Exec()
	return
}

// Get value by key, returns key value or empty data if key not found
func (m *Map) Get(key string) (data []byte, err error) {
	// Does not return err of cdb.session.Query function
	err = m.session.Query(`SELECT data FROM map WHERE key = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&data)
	return
}

// Delete record from database by key, returns
func (m *Map) Delete(key string) (err error) {
	// Does not return err of cdb.session.Query function
	err = m.session.Query(`DELETE FROM map WHERE key = ?`,
		key).Exec()
	return
}

// List read and return array of all keys starts from selected key
func (m *Map) List(key string) (keyList KeyList, err error) {
	var keyOut string
	iter := m.session.Query(`
		SELECT key FROM map WHERE key >= ? and key < ?
		ALLOW FILTERING`,
		key, key+"a").Iter()
	for iter.Scan(&keyOut) {
		keyList.Append(keyOut)
	}
	return
}

// ListBody read and return array of all keys data starts from selected key
func (m *Map) ListBody(key string) (dataList [][]byte, err error) {
	iter := m.session.Query(`
		SELECT data FROM map WHERE key >= ? and key < ?
		ALLOW FILTERING`,
		key, key+"a").Iter()
	for {
		var dataOut []byte
		if !iter.Scan(&dataOut) {
			break
		}
		dataList = append(dataList, dataOut)
	}
	return
}
