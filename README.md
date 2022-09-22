# Golang AWS Keyspaces make KeyValue Database helper package

The kscdb golang package create connection to AWS Keyspaces and provides functions
to use AWS Keyspaces as KeyValue database, create and manage IDs, use Named Queue
and call remote functions defined in go plugins.

[![GoDoc](https://godoc.org/github.com/kirill-scherba/kscdb?status.svg)](https://godoc.org/github.com/kirill-scherba/kscdb/)
[![Go Report Card](https://goreportcard.com/badge/github.com/kirill-scherba/kscdb)](https://goreportcard.com/report/github.com/kirill-scherba/kscdb)

## Run example

The `keyvalue` packages example connect to AWS Keyspaces, save KeyValue to
Keyspaces and than read it. Than save another one KeyValue and read list of
keys and list of values. To execute this example you need create `kscdb`
keyspace in your AWS Keyspaces and get your AWS Keyspaces credentials in AWS
Users Page.

The `-username`, `-passwd` and `-host` is requered parameters, or you can use
environment variables instead:

    KEYSPACES_USERNAME - keyspaces user name
    KEYSPACES_PASSWD - keyspaces parrword
    KEYSPACES_HOST - keyspaces host

Execute next command to run this example:

    go run ./cmd/keyvalue

## Licence

[BSD](LICENSE)
