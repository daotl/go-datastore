// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package mount

import (
	"testing"

	key "github.com/bdware/go-datastore/key"
	dstest "github.com/bdware/go-datastore/test"
)

func TestLookup(t *testing.T) {
	mapds0 := dstest.NewMapDatastoreForTest(t)
	mapds1 := dstest.NewMapDatastoreForTest(t)
	mapds2 := dstest.NewMapDatastoreForTest(t)
	mapds3 := dstest.NewMapDatastoreForTest(t)
	m := New([]Mount{
		{Prefix: key.NewStrKey("/"), Datastore: mapds0},
		{Prefix: key.NewStrKey("/foo"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/bar"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/baz"), Datastore: mapds3},
	})
	_, mnts, _ := m.lookupAll(key.NewStrKey("/bar"))
	if len(mnts) != 1 || mnts[0] != key.NewStrKey("/bar") {
		t.Errorf("expected to find the mountpoint /bar, got %v", mnts)
	}

	_, mnts, _ = m.lookupAll(key.NewStrKey("/fo"))
	if len(mnts) != 1 || mnts[0] != key.NewStrKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnts)
	}

	_, mnt, _ := m.lookup(key.NewStrKey("/fo"))
	if mnt != key.NewStrKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	// /foo lives in /, /foo/bar lives in /foo. Most systems don't let us use the key "" or /.
	_, mnt, _ = m.lookup(key.NewStrKey("/foo"))
	if mnt != key.NewStrKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	_, mnt, _ = m.lookup(key.NewStrKey("/foo/bar"))
	if mnt != key.NewStrKey("/foo") {
		t.Errorf("expected to find the mountpoint /foo, got %v", mnt)
	}
}
