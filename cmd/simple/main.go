// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

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
	appVersion = "0.0.1"
	appDescr   = "AWS Keyspaces KeyValue sample application"
)

// Application parameters type
type Params struct {
	username, passwd, host string
}

// Application parameters
var params Params

func main() {

	// Application logo
	fmt.Println(appDescr + " ver. " + appVersion)

	// Parse application command line parameters
	flag.StringVar(&params.username, "username", os.Getenv("KEYSPACES_USERNAME"), "Keyspaces user name")
	flag.StringVar(&params.passwd, "passwd", os.Getenv("KEYSPACES_PASSWD"), "Keyspaces user password")
	flag.StringVar(&params.host, "host", os.Getenv("KEYSPACES_HOST"), "Keyspaces host")
	//
	flag.Parse()

	// Check requered application parameters
	if len(params.username) == 0 || len(params.passwd) == 0 || len(params.host) == 0 {
		fmt.Println(
			"The username, passwd and host is requered parameters",
			"\nor you can use environment variables:",
			"\n  KEYSPACES_USERNAME - keyspaces user name",
			"\n  KEYSPACES_PASSWD - keyspaces parrword",
			"\n  KEYSPACES_HOST - keyspaces host",
		)
		flag.Usage()
		return
	}

	// Connect to AWS keyspaces
	log.Println("Start connection to AWS Keyspaces")
	cdb, err := kscdb.Connect(params.username, params.passwd, params.host)
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
}
