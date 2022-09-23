// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The `keyvalue` packages example connect to AWS Keyspaces, save KeyValue to
// Keyspaces and than read it. Than save another one KeyValue and read list of
// keys and list of values.
// To execute this example you need create `kscdb` keyspace in
// your AWS Keyspaces and get your AWS Keyspaces credentials in AWS Users Page.
//
// The `-host` and `-keyspace` is requered parameters. To define host you can
// use environment variable `KEYSPACES_HOST`. The keyspace has default value:
// `kscdb`.
//
// Execute next command to run this example:
//
//	go run ./examples/keyvalue
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/kirill-scherba/kscdb"
)

const (
	appName    = "kscdb"
	appVersion = kscdb.Version
	appDescr   = "AWS Keyspaces KeyValue sample application"
)

// Application parameters type
type Params struct {
	// username, passwd string
	keyspace string
	host     string
	aws      bool
}

// Application parameters
var params Params

func main() {

	// Application logo
	fmt.Println(appDescr + " ver " + appVersion)

	// Parse application command line parameters
	// flag.StringVar(&params.username, "username", os.Getenv("KEYSPACES_USERNAME"), "Keyspaces user name")
	// flag.StringVar(&params.passwd, "passwd", os.Getenv("KEYSPACES_PASSWD"), "Keyspaces user password")
	flag.StringVar(&params.keyspace, "keyspace", "kscdb", "keyspace name")
	flag.StringVar(&params.host, "host", os.Getenv("KEYSPACES_HOST"), "connect to host name")
	flag.BoolVar(&params.aws, "aws", true, "connect to AWS Keyspaces")

	//
	flag.Parse()

	// Check requered application parameters
	if len(params.host) == 0 {
		fmt.Println(
			"The host is requered parameters",
			"\nor you can use environment variables:",
			"\n  KEYSPACES_HOST - keyspaces host",
		)
		flag.Usage()
		return
	}

	// Connect to AWS keyspaces
	log.Println("Start connection to AWS Keyspaces")
	cdb, err := kscdb.Connect(params.keyspace, params.aws, params.host)
	if err != nil {
		panic(err)
	}
	defer cdb.Close()
	log.Println("Connected to AWS Keyspaces")

	// Set test value
	key := "/test/key/001"
	value := []byte("Some test value")
	err = cdb.Set(key, value)
	if err != nil {
		panic(err)
	}
	log.Println("Set test key", key)

	// Get test value
	// You can check this value in AWS Keyspace CQL Editor with command:
	//   select blobAsText(data) from kscdb.map where key='/test/key/001'
	data, err := cdb.Get(key)
	if err != nil {
		panic(err)
	}
	log.Println("Get test key value:", string(data))

	// Add next one key value
	key = "/test/key/002"
	value = []byte("Some next test value")
	err = cdb.Set(key, value)
	if err != nil {
		panic(err)
	}
	log.Println("Set test key", key)

	// Get list of keys
	key = "/test/key/"
	keyList, err := cdb.List(key)
	if err != nil {
		panic(err)
	}
	log.Printf("Get keys list:\n%s\n", keyList.String())

	// Get list with body
	values, err := cdb.ListBody(key)
	if err != nil {
		panic(err)
	}
	log.Println("Get values list:")
	for _, value := range values {
		fmt.Println(string(value))
	}

	// Test lock/unlock
	log.Println("Lock")
	lockKey := "/test/lock/001"
	lockid, err := cdb.Lock(lockKey)
	fmt.Println("lock:", lockKey, lockid, err)

	log.Println("Unlock")
	err = cdb.Unlock(lockKey, lockid)
	fmt.Println("unlock:", lockKey, lockid, err)
}
