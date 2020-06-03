package sync

import (
	"testing"

	ds "github.com/bdware/go-datastore"
	dstest "github.com/bdware/go-datastore/test"
)

func TestSync(t *testing.T) {
	dstest.SubtestAll(t, MutexWrap(ds.NewMapDatastore()))
}
