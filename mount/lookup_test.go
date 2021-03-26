// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package mount

import (
	"testing"

	key "github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"
)

func testLookup(t *testing.T, keyType key.KeyType) {
	mapds0 := dstest.NewMapDatastoreForTest(t, keyType)
	mapds1 := dstest.NewMapDatastoreForTest(t, keyType)
	mapds2 := dstest.NewMapDatastoreForTest(t, keyType)
	mapds3 := dstest.NewMapDatastoreForTest(t, keyType)
	m := New([]Mount{
		{Prefix: key.NewKeyFromTypeAndString(keyType, "/"), Datastore: mapds0},
		{Prefix: key.NewKeyFromTypeAndString(keyType, "/foo"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(keyType, "/bar"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(keyType, "/baz"), Datastore: mapds3},
	})
	_, mnts, _, _ := m.lookupAll(key.NewKeyFromTypeAndString(keyType, "/bar"), dsq.Range{})
	if len(mnts) != 1 || !mnts[0].Equal(key.NewKeyFromTypeAndString(keyType, "/bar")) {
		t.Errorf("expected to find the mountpoint /bar, got %v", mnts)
	}

	if keyType == key.KeyTypeString {
		_, mnts, _, _ = m.lookupAll(key.NewKeyFromTypeAndString(keyType, "/fo"), dsq.Range{})
		if len(mnts) != 1 || !mnts[0].Equal(key.NewKeyFromTypeAndString(keyType, "/")) {
			t.Errorf("expected to find the mountpoint /, got %v", mnts)
		}

		_, mnt, _ := m.lookup(key.NewKeyFromTypeAndString(keyType, "/fo"))
		if !mnt.Equal(key.NewKeyFromTypeAndString(keyType, "/")) {
			t.Errorf("expected to find the mountpoint /, got %v", mnt)
		}
	}

	// /foo lives in /, /foo/bar lives in /foo. Most systems don't let us use the key "" or /.
	_, mnt, _ := m.lookup(key.NewKeyFromTypeAndString(keyType, "/foo"))
	if !mnt.Equal(key.NewKeyFromTypeAndString(keyType, "/")) {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	_, mnt, _ = m.lookup(key.NewKeyFromTypeAndString(keyType, "/foo/bar"))
	if !mnt.Equal(key.NewKeyFromTypeAndString(keyType, "/foo")) {
		t.Errorf("expected to find the mountpoint /foo, got %v", mnt)
	}
}

func TestLookup(t *testing.T) {
	testLookup(t, key.KeyTypeString)
	testLookup(t, key.KeyTypeBytes)
}
