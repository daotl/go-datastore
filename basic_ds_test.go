// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package datastore_test

import (
	"io/ioutil"
	"log"
	"testing"

	dstore "github.com/daotl/go-datastore"
	dstest "github.com/daotl/go-datastore/test"
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
