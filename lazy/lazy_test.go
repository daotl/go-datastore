package lazy

import (
	"testing"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dstest "github.com/daotl/go-datastore/test"
)

func TestLazy(t *testing.T) {
	tds := dstest.NewMapDatastoreForTest(t, key.KeyTypeString)
	init := func(d *datastore.Datastore) error {
		return nil
	}
	activate := func(d *datastore.Datastore) error {
		*d = tds
		return nil
	}
	deactivate := func(d *datastore.Datastore) error {
		*d = nil
		return nil
	}
	close := func(d *datastore.Datastore) error {
		return nil
	}
	ds, err := NewLazyDataStore(init, activate, deactivate, close)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	dstest.SubtestAll(t, key.KeyTypeString, ds)
}
