package sync

import (
	"testing"

	dstest "github.com/bdware/go-datastore/test"
)

func TestSync(t *testing.T) {
	dstest.SubtestAll(t, MutexWrap(dstest.NewMapDatastoreForTest(t)))
}
