package datastore_test

import (
	"io/ioutil"
	"log"
	"testing"

	dstore "github.com/bdware/go-datastore"
	dstest "github.com/bdware/go-datastore/test"
)

func TestMapDatastore(t *testing.T) {
	ds := dstest.NewMapDatastoreForTest(t)
	dstest.SubtestAll(t, ds)
}

func TestNullDatastore(t *testing.T) {
	ds := dstore.NewNullDatastore()
	// The only test that passes. Nothing should be found.
	dstest.SubtestNotFounds(t, ds)
}

func TestLogDatastore(t *testing.T) {
	defer log.SetOutput(log.Writer())
	log.SetOutput(ioutil.Discard)
	ds := dstore.NewLogDatastore(dstest.NewMapDatastoreForTest(t), "")
	dstest.SubtestAll(t, ds)
}
