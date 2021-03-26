// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package dstest

import (
	"reflect"
	"runtime"
	"testing"

	dstore "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	query "github.com/daotl/go-datastore/query"
)

// BasicSubtests is a list of all basic tests.
var BasicSubtests = []func(t *testing.T, keyType key.KeyType, ds dstore.Datastore){
	SubtestBasicPutGet,
	SubtestNotFounds,
	SubtestCombinations,
	SubtestPrefix,
	SubtestOrder,
	SubtestLimit,
	SubtestFilter,
	SubtestManyKeysAndQuery,
	SubtestReturnSizes,
	SubtestBasicSync,
}

// BatchSubtests is a list of all basic batching datastore tests.
var BatchSubtests = []func(t *testing.T, keyType key.KeyType, ds dstore.Batching){
	RunBatchTest,
	RunBatchDeleteTest,
	RunBatchPutAndDeleteTest,
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func clearDs(t *testing.T, ds dstore.Datastore) {
	q, err := ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	res, err := q.Rest()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if err := ds.Delete(r.Key); err != nil {
			t.Fatal(err)
		}
	}
}

// SubtestAll tests the given datastore against all the subtests.
func SubtestAll(t *testing.T, keyType key.KeyType, ds dstore.Datastore) {
	for _, f := range BasicSubtests {
		t.Run(getFunctionName(f), func(t *testing.T) {
			f(t, keyType, ds)
			clearDs(t, ds)
		})
	}
	if ds, ok := ds.(dstore.Batching); ok {
		for _, f := range BatchSubtests {
			t.Run(getFunctionName(f), func(t *testing.T) {
				f(t, keyType, ds)
				clearDs(t, ds)
			})
		}
	}
}
