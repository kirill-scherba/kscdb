// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// kscdb package contains Golang functions to rasy use AWS Keyspaces as
// KeyValue Database. To use it create next keyspace in your AWS Keyspaces:
//
//	kscdb
package kscdb

import (
	"context"
	"embed"
	"os"
	"plugin"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
)

const Version = "0.0.4"

// Kscdb is kscdb packet receiver
type Kscdb struct {
	session *gocql.Session
	aws     bool
	ID      IDs
	Map     Map
	Queue   Queue
}

//go:embed crt
var f embed.FS

const crtFileName = "sf-class2-root.crt"

// Connect to the cql cluster and return kscdb receiver
func Connect(keyspace string, aws bool, hosts ...string) (cdb *Kscdb, err error) {

	cdb = new(Kscdb)
	cdb.aws = aws
	cdb.ID.Kscdb = cdb
	cdb.Map.Kscdb = cdb
	cdb.Queue.Kscdb = cdb

	// Add the keyspaces service endpoint
	cluster := gocql.NewCluster(hosts...)

	// For AWS Keyspaces
	if aws {

		// Port used when dialing
		cluster.Port = 9142

		// add your service specific credentials
		// cluster.Authenticator = gocql.PasswordAuthenticator{
		// 	Username: username,
		// 	Password: passwd,
		// }

		// Get credentails from AWS Config
		cfg, errCfg := config.LoadDefaultConfig(context.TODO())
		if errCfg != nil {
			err = errCfg
			return
		}
		cre, errCfg := cfg.Credentials.Retrieve(context.TODO())
		if errCfg != nil {
			err = errCfg
			return
		}

		// Use the SigV4 AWS authentication plugin
		var auth sigv4.AwsAuthenticator = sigv4.NewAwsAuthenticator()
		auth.Region = cfg.Region
		auth.AccessKeyId = cre.AccessKeyID
		auth.SecretAccessKey = cre.SecretAccessKey
		cluster.Authenticator = auth

		// Get certificate file from embed fs and write it to tmp folder
		var data []byte
		data, err = f.ReadFile("crt/" + crtFileName)
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
		create TABLE IF NOT EXISTS ` + keyspace + `.map(
			key text,
			data blob,
			PRIMARY KEY(key)
		);`, `
		create TABLE IF NOT EXISTS ` + keyspace + `.ids(
			id_name text,
			next_id int,
			PRIMARY KEY(id_name)
		);`, `
		create TABLE IF NOT EXISTS ` + keyspace + `.queue(
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
