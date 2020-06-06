// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package delayed

import (
	"testing"
	"time"

	datastore "github.com/bdware/go-datastore"
	key "github.com/bdware/go-datastore/key"
	dstest "github.com/bdware/go-datastore/test"
	delay "github.com/ipfs/go-ipfs-delay"
)

func TestDelayed(t *testing.T) {
	d := New(dstest.NewMapDatastoreForTest(t), delay.Fixed(time.Second))
	now := time.Now()
	k := key.NewStrKey("test")
	err := d.Put(k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(k)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(now) < 2*time.Second {
		t.Fatal("There should have been a delay of 1 second in put and in get")
	}
}

func TestDelayedAll(t *testing.T) {
	ds, err := datastore.NewMapDatastore(key.KeyTypeString)
	if err != nil {
		t.Fatal("error creating MapDatastore: ", err)
	}
	dstest.SubtestAll(t, New(ds, delay.Fixed(time.Millisecond)))
}
