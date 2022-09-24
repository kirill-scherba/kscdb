// Execute next command to run this example:
//
//	go run ./examples/queue
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
	appDescr   = "AWS Keyspaces Named Queue sample application"
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

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Connect to AWS keyspaces
	log.Println("Start connection to AWS Keyspaces")
	cdb, err := kscdb.Connect(params.keyspace, params.aws, params.host)
	if err != nil {
		panic(err)
	}
	defer cdb.Close()
	log.Println("Connected to AWS Keyspaces")

	key := "/test/queue/002"
	cdb.Queue.Clear(key)
	cdb.Unlock(key)

	// Add values to name queue
	fmt.Println()
	num := 5
	for i := 1; i <= num; i++ {
		value := fmt.Sprintf("My %d value", i)
		log.Println("Set value:", value)
		err := cdb.Queue.Set(key, []byte(value))
		if err != nil {
			panic("set value error: " + err.Error())
		}
	}

	// Getvalues from name queue
	fmt.Println()
	var wg sync.WaitGroup
	for i := 1; i <= num; i++ {
		wg.Add(1)
		go func() {
			data, err := cdb.Queue.Get(key)
			if err != nil {
				if err == kscdb.ErrNotFound {
					// break
					return
				}
				panic("Get value error: " + err.Error())
			}
			log.Println("Get value:", string(data))
			wg.Done()
		}()
	}
	wg.Wait()
}
