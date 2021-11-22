package dstest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	dstore "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
)

// TxnSubtests is a list of all txn datastore tests.
var TxnSubtests = []func(t *testing.T, ktype key.KeyType, ds dstore.TxnDatastore){
	SubtestTxnBasicPutGet,
	SubtestTxnDiscard,
	SubtestTxnCommit,
	SubtestTxnQuery,
}

func SubtestTxnBasicPutGet(t *testing.T, ktype key.KeyType, ds dstore.TxnDatastore) {
	ctx := context.Background()

	txn, err := ds.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal("error creating txn: ", err)
	}
	subtestBasicPutGet(t, ktype, txn)
	if err := txn.Commit(ctx); err != nil {
		t.Fatal("error committing txn: ", err)
	}
}

func SubtestTxnDiscard(t *testing.T, ktype key.KeyType, ds dstore.TxnDatastore) {
	ctx := context.Background()

	// put a value, then abort the txn, the following get must not see the update
	txn, err := ds.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal("error creating txn: ", err)
	}
	k := key.NewKeyFromTypeAndString(ktype, "foo")
	val := []byte("Hello Datastore!")

	if err := txn.Put(ctx, k, val); err != nil {
		t.Fatal("error putting to datastore: ", err)
	}
	txn.Discard(ctx)
	_, err = ds.Get(ctx, k)
	if err != dstore.ErrNotFound {
		t.Fatal("error calling get on key we just discard: ", err)
	}
}

func SubtestTxnCommit(t *testing.T, ktype key.KeyType, ds dstore.TxnDatastore) {
	ctx := context.Background()

	// put a value, then commit the txn, the following get must see the update
	txn, err := ds.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal("error creating txn: ", err)
	}
	k := key.NewKeyFromTypeAndString(ktype, "foo")
	val := []byte("Hello Datastore!")

	if err := txn.Put(ctx, k, val); err != nil {
		t.Fatal("error putting to datastore: ", err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal("error committing the txn: ", err)
	}
	out, err := ds.Get(ctx, k)

	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get was not what we expected:", out)
	}
}

func SubtestTxnQuery(t *testing.T, ktype key.KeyType, ds dstore.TxnDatastore) {
	ctx := context.Background()

	offsets, limits, filters, prefixes, orders, lengths := genQueryConditions(ktype)
	for _, length := range lengths {
		txn, err := ds.NewTransaction(ctx, false)
		if err != nil {
			t.Fatal("error creating txn", err)
		}
		input := prepareDs(t, ktype, txn, length)
		if err := txn.Commit(ctx); err != nil {
			t.Fatal("error committing txn", err)
		}

		readTx, err := ds.NewTransaction(ctx, true)
		if err != nil {
			t.Fatal("error creating txn", err)
		}
		perms(
			func(perm []int) {
				q := dsq.Query{
					Offset:  offsets[perm[0]],
					Limit:   limits[perm[1]],
					Filters: filters[perm[2]],
					Orders:  orders[perm[3]],
					Prefix:  prefixes[perm[4]],
				}

				t.Run(strings.ReplaceAll(fmt.Sprintf("%d/{%s}", length, q), " ", "·"), func(t *testing.T) {
					subtestQuery(t, ktype, readTx, q, input)
				})
			},
			len(offsets),
			len(limits),
			len(filters),
			len(orders),
			len(prefixes),
		)
		readTx.Discard(ctx)

		delTxn, err := ds.NewTransaction(ctx, false)
		if err != nil {
			t.Fatal("error creating txn", err)
		}
		deleteAll(t, delTxn, input)
		if err := delTxn.Commit(ctx); err != nil {
			t.Fatal("error committing txn", err)
		}
	}
}
