// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Execute next command to run this example:
//
//	go run ./examples/id
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

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
	// flag.StringVar(&params.username, "username", os.Getenv("KEYSPACES_USERNAME"), "user name")
	// flag.StringVar(&params.passwd, "passwd", os.Getenv("KEYSPACES_PASSWD"), "user password")
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

	// Create ID
	key := "/test/id/001"
	value := []byte("1")
	cdb.ID.Set(key, value)
	if err != nil {
		panic(err)
	}
	log.Println("Set ID", key, "to", string(value))

	// Reset lock
	// cdb.Map.Delete("/test/id/001/lock")
	cdb.Unlock(key)

	// Get 10 new IDs parallel
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Println("Request Next ID,", "loop:", i)
			data, err := cdb.ID.Get(key)
			if err != nil {
				log.Println("GetID error:", key, err)
				return
			}
			log.Println("Get loop:", i, "Next ID:",string(data))
		}(i)
	}
	wg.Wait()
}
