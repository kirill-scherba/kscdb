// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// kscdb package contains Golang functions to rasy use AWS Keyspaces as
// KeyValue Database. To use it create next keyspace in your AWS Keyspaces:
//
//	kscdb
package kscdb

import (
	"embed"
	"fmt"
	"log"
	"os"
	"plugin"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

// Kscdb is kscdb packet receiver
type Kscdb struct {
	session *gocql.Session
}

//go:embed crt
var f embed.FS

const crtFileName = "sf-class2-root.crt"

// Connect to the cql cluster and return kscdb receiver
func Connect(username, passwd string, hosts ...string) (cdb *Kscdb, err error) {

	cdb = new(Kscdb)
	const keyspace = "kscdb"

	// add the Amazon Keyspaces service endpoint
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = 9142

	// add your service specific credentials
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: passwd,
	}

	// Get certificate file from embed fs and write it to tmp folder
	data, err := f.ReadFile("crt/" + crtFileName)
	if err != nil {
		return
	}
	err = os.WriteFile("/tmp/"+crtFileName, data, 0777)
	if err != nil {
		return
	}

	// Provide the path to the sf-class2-root.crt certificate file
	cluster.SslOpts = &gocql.SslOptions{
		CaPath:                 "/tmp/" + crtFileName,
		EnableHostVerification: false,
	}

	// Override default Consistency to LocalQuorum
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.DisableInitialHostLookup = false

	// Create session
	if cdb.session, err = cluster.CreateSession(); err != nil {
		return
	}

	// Create tables if not exists
	// create KEYSPACE IF NOT EXISTS kscdb WITH replication = {
	// 	'class' : 'SimpleStrategy',
	// 	'replication_factor' : 3
	// };
	var tables = []string{`
		create TABLE IF NOT EXISTS kscdb.map(
			key text,
			data blob,
			PRIMARY KEY(key)
		);`, `
		create TABLE IF NOT EXISTS kscdb.ids(
			id_name text,
			next_id int,
			PRIMARY KEY(id_name)
		);`, `
		create TABLE IF NOT EXISTS kscdb.queue(
			key text, time timestamp, 
			random UUID, lock text, 
			data blob, 
			PRIMARY KEY(key, time, random)
		);
		`,
	}
	for _, table := range tables {
		if err = cdb.execStmt(table); err != nil {
			return
		}
	}

	return
}

// ExecStmt executes a statement string.
func (cdb *Kscdb) execStmt(stmt string) error {
	q := cdb.session.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	return q.Exec()
}

// Cloase kscdb connection
func (cdb *Kscdb) Close() {
	cdb.session.Close()
}

// Set key value
func (cdb *Kscdb) Set(key string, value []byte) (err error) {
	err = cdb.session.Query(`UPDATE map SET data = ? WHERE key = ?`,
		value, key).Exec()
	return
}

// Get value by key, returns key value or empty data if key not found
func (cdb *Kscdb) Get(key string) (data []byte, err error) {
	// Does not return err of cdb.session.Query function
	err = cdb.session.Query(`SELECT data FROM map WHERE key = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&data)
	return
}

// Delete record from database by key, returns
func (cdb *Kscdb) Delete(key string) (err error) {
	// Does not return err of cdb.session.Query function
	err = cdb.session.Query(`DELETE data FROM map WHERE key = ?`,
		key).Exec()
	return
}

// List read and return array of all keys starts from selected key
func (cdb *Kscdb) List(key string) (keyList KeyList, err error) {
	var keyOut string
	iter := cdb.session.Query(`
		SELECT key FROM map WHERE key >= ? and key < ?
		ALLOW FILTERING`,
		key, key+"a").Iter()
	for iter.Scan(&keyOut) {
		keyList.Append(keyOut)
	}
	return
}

// ListBody read and return array of all keys data starts from selected key
func (cdb *Kscdb) ListBody(key string) (dataList []string, err error) {
	var dataOut string
	iter := cdb.session.Query(`
		SELECT data FROM map WHERE key >= ? and key < ?
		ALLOW FILTERING`,
		key, key+"a").Iter()
	for iter.Scan(&dataOut) {
		dataList = append(dataList, dataOut)
	}
	return
}

// Func execute plugin function and return data
func (cdb *Kscdb) Func(key string, value []byte) (data []byte, err error) {
	return cdb.PluginFunc(key, value)
}

// PluginFunc process plugin function: plugin_name.func(parameters ...string)
func (tcdb *Kscdb) PluginFunc(fff string, value []byte) (data []byte, err error) {

	d := Plugin{}
	d.UnmarshalBinary(value)

	p, err := plugin.Open("/root/plugin/" + d.Name + ".so")
	if err != nil {
		return
	}

	f, err := p.Lookup(d.Func)
	if err != nil {
		return
	}

	return f.(func(*Kscdb, ...string) ([]byte, error))(tcdb, d.Params...)
}

// SetID set keys next ID value
func (cdb *Kscdb) SetID(key string, value []byte) (err error) {
	nextID, err := strconv.Atoi(string(value))
	if err != nil {
		return
	}
	return cdb.session.Query(`UPDATE ids SET next_id = ? WHERE id_name = ?`,
		nextID, key).Exec()
}

// GetID returns new diginal ID for key, ID just increments
func (cdb *Kscdb) GetID(key string) (data []byte, err error) {
	var nextID int
	// Read current counter value with id_name
	if err = cdb.session.Query(`SELECT next_id FROM ids WHERE id_name = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&nextID); err != nil {

		// Check error
		if err != gocql.ErrNotFound {
			log.Println("Read current counter error:", err)
			return
		}

		// Create new record if counter with id_name does not exists
		nextID = 1
		if err = cdb.SetID(key, []byte(strconv.Itoa(nextID))); err != nil {
			return
		}
	}

	// Set new next_id and return current next_id
	var ok bool
	for {
		// Increment nextID in database
		if err = cdb.session.Query(
			`UPDATE ids SET next_id = ? WHERE id_name = ? IF next_id = ?`,
			nextID+1, key, nextID).Scan(&ok, &nextID); err != nil {
			log.Println("Increment current counter error:", err)
			return
		}
		// log.Println("Update result:", ok, nextID)
		if ok {
			break
		}
	}

	// Return received nextID in text
	data = []byte(fmt.Sprintf("%d", nextID))
	return
}

// DeleteID delete counter from database by key
func (cdb *Kscdb) DeleteID(key string) (err error) {
	return cdb.session.Query(`DELETE FROM ids WHERE id_name = ?`,
		key).Exec()
}

// SetQueue add value to named queue by key (name of queue)
func (cdb *Kscdb) SetQueue(key string, value []byte) (err error) {
	return cdb.session.Query(
		`UPDATE queue SET lock = '', data = ? WHERE key = ? AND time = toTimestamp(now()) AND random = UUID()`,
		value, key).Exec()
}

// GetQueue get first value from named queue by key (name of queue)
func (cdb *Kscdb) GetQueue(key string) (data []byte, err error) {
	// Get free value
	var time time.Time
	var random string
	if err = cdb.session.Query(
		`SELECT time, random, data FROM queue WHERE key = ? AND lock = '' LIMIT 1 ALLOW FILTERING`,
		key).Consistency(gocql.One).Scan(&time, &random, &data); err != nil {
		return
	}

	// Loc record (to allow concurency)
	var ok bool
	var lock string
	if err = cdb.session.Query(
		`UPDATE queue SET lock = 'locked' WHERE key = ? AND time = ? AND random = ? IF lock = ''`,
		key, time, random).Consistency(gocql.One).Scan(&ok, &lock); err != nil {
		return
	}
	if !ok {
		return cdb.GetQueue(key)
	}

	// Delete locket record from queue and return value
	err = cdb.session.Query(
		`DELETE FROM queue WHERE key = ? AND time = ? AND random = ?`,
		key, time, random).Exec()
	return
}
