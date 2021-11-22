// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package dstest

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	detectrace "github.com/ipfs/go-detect-race"

	dstore "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
)

type readWrite interface {
	dstore.Read
	dstore.Write
}

// ElemCount sets with how many elements the datastore suit
// tests are usually run with. Best to set to round numbers like
// 20, 30, 40... and at least to 20.
var ElemCount = 100

func init() {
	// Reduce the default element count when the race detector is enabled so these tests don't
	// take forever.
	if detectrace.WithRace() {
		ElemCount = 20
	}
}

func TestElemCount(t *testing.T) {
	if ElemCount < 20 {
		t.Fatal("ElemCount should be set to 20 at least")
	}
}

func SubtestBasicPutGet(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	subtestBasicPutGet(t, ktype, ds)
}

func subtestBasicPutGet(t *testing.T, ktype key.KeyType, ds readWrite) {
	ctx := context.Background()

	k := key.NewKeyFromTypeAndString(ktype, "foo")
	val := []byte("Hello Datastore!")

	err := ds.Put(ctx, k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := ds.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	size, err := ds.GetSize(ctx, k)
	if err != nil {
		t.Fatal("error getting size after put: ", err)
	}
	if size != len(val) {
		t.Fatalf("incorrect size: expected %d, got %d", len(val), size)
	}

	out, err := ds.Get(ctx, k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = ds.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	size, err = ds.GetSize(ctx, k)
	if err != nil {
		t.Fatal("error getting size after get: ", err)
	}
	if size != len(val) {
		t.Fatalf("incorrect size: expected %d, got %d", len(val), size)
	}

	err = ds.Delete(ctx, k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = ds.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}

	size, err = ds.GetSize(ctx, k)
	switch err {
	case dstore.ErrNotFound:
	case nil:
		t.Fatal("expected error getting size after delete")
	default:
		t.Fatal("wrong error getting size after delete: ", err)
	}
	if size != -1 {
		t.Fatal("expected missing size to be -1")
	}
}

func SubtestNotFounds(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	ctx := context.Background()

	badk := key.NewKeyFromTypeAndString(ktype, "notreal")

	val, err := ds.Get(ctx, badk)
	if err != dstore.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := ds.Has(ctx, badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}

	size, err := ds.GetSize(ctx, badk)
	switch err {
	case dstore.ErrNotFound:
	case nil:
		t.Fatal("expected error getting size of not found key")
	default:
		t.Fatal("wrong error getting size of not found key", err)
	}
	if size != -1 {
		t.Fatal("expected missing size to be -1")
	}

	err = ds.Delete(ctx, badk)
	if err != nil {
		t.Fatal("error calling delete on not found key: ", err)
	}
}

func SubtestRange(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)
	test := func(start, end string) {
		t.Run(fmt.Sprintf("Range/%s/%s", start, end), func(t *testing.T) {
			q := dsq.Query{
				KeysOnly: true,
			}
			if start != "" {
				q.Range.Start = key.NewKeyFromTypeAndString(ktype, start)
			}
			if end != "" {
				q.Range.End = key.NewKeyFromTypeAndString(ktype, end)
			}
			subtestQuery(t, ktype, ds, q, input)
		})
	}
	test("", "")
	test("6", "")
	test("", "/prefj")
	test("/prefix/3", "/prefix/9999")
}

func SubtestLimit(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)

	test := func(offset, limit int) {
		t.Run(fmt.Sprintf("Slice/%d/%d", offset, limit), func(t *testing.T) {
			subtestQuery(t, ktype, ds, dsq.Query{
				Orders:   []dsq.Order{dsq.OrderByKey{}},
				Offset:   offset,
				Limit:    limit,
				KeysOnly: true,
			}, input)
		})
	}
	test(0, ElemCount/10)
	test(0, 0)
	test(ElemCount/10, 0)
	test(ElemCount/10, ElemCount/10)
	test(ElemCount/10, ElemCount/5)
	test(ElemCount/2, ElemCount/5)
	test(ElemCount-1, ElemCount/5)
	test(ElemCount*2, ElemCount/5)
	test(ElemCount*2, 0)
	test(ElemCount-1, 0)
	test(ElemCount-5, 0)
	deleteAll(t, ds, input)
}

func SubtestOrder(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)

	test := func(orders ...dsq.Order) {
		var types []string
		for _, o := range orders {
			types = append(types, fmt.Sprintf("%T", o))
		}
		name := strings.Join(types, ">")
		t.Run(name, func(t *testing.T) {
			subtestQuery(t, ktype, ds, dsq.Query{
				Orders: orders,
			}, input)
		})
	}
	test(dsq.OrderByKey{})
	test(new(dsq.OrderByKey))
	test(dsq.OrderByKeyDescending{})
	test(new(dsq.OrderByKeyDescending))
	test(dsq.OrderByValue{})
	test(dsq.OrderByValue{}, dsq.OrderByKey{})
	test(dsq.OrderByFunction(func(a, b dsq.Entry) int {
		return bytes.Compare(a.Value, b.Value)
	}))
	deleteAll(t, ds, input)

}

func SubtestManyKeysAndQuery(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)
	subtestQuery(t, ktype, ds, dsq.Query{KeysOnly: true}, input)
	deleteAll(t, ds, input)
}

func SubtestBasicSync(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	ctx := context.Background()

	if err := ds.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "prefix")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/prefix"), []byte("foo")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "/prefix")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/prefix/sub"),
		[]byte("bar")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "/prefix")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "/prefix/sub")); err != nil {
		t.Fatal(err)
	}

	if err := ds.Sync(ctx, key.EmptyKeyFromType(ktype)); err != nil {
		t.Fatal(err)
	}
}

// need a custom test filter to test the "fallback" filter case for unknown
// filters.
type testFilter struct{}

func (testFilter) Filter(e dsq.Entry) bool {
	return len(e.Key.Bytes())%2 == 0
}

func SubtestCombinations(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	offsets, limits, filters, prefixes, orders, lengths := genQueryConditions(ktype)

	for _, length := range lengths {
		input := prepareDs(t, ktype, ds, length)

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
					subtestQuery(t, ktype, ds, q, input)
				})
			},
			len(offsets),
			len(limits),
			len(filters),
			len(orders),
			len(prefixes),
		)
		deleteAll(t, ds, input)
	}
}

func perms(cb func([]int), ops ...int) {
	current := make([]int, len(ops))
outer:
	for {
		for i := range current {
			if current[i] < (ops[i] - 1) {
				current[i]++
				cb(current)
				continue outer
			}
			current[i] = 0
		}
		// out of permutations
		return
	}
}

func SubtestFilter(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)

	test := func(filters ...dsq.Filter) {
		var types []string
		for _, o := range filters {
			types = append(types, fmt.Sprintf("%T", o))
		}
		name := strings.Join(types, ">")
		t.Run(name, func(t *testing.T) {
			subtestQuery(t, ktype, ds, dsq.Query{
				Filters: filters,
			}, input)
		})
	}
	test(dsq.FilterKeyCompare{
		Op:  dsq.Equal,
		Key: key.QueryKeyFromTypeAndString(ktype, "/0key0"),
	})

	test(dsq.FilterKeyCompare{
		Op:  dsq.LessThan,
		Key: key.QueryKeyFromTypeAndString(ktype, "/2"),
	})

	test(&dsq.FilterKeyCompare{
		Op:  dsq.Equal,
		Key: key.QueryKeyFromTypeAndString(ktype, "/0key0"),
	})

	test(dsq.FilterKeyPrefix{
		Prefix: key.QueryKeyFromTypeAndString(ktype, "/0key0"),
	})

	test(&dsq.FilterKeyPrefix{
		Prefix: key.QueryKeyFromTypeAndString(ktype, "/0key0"),
	})

	test(dsq.FilterValueCompare{
		Op:    dsq.LessThan,
		Value: randValue(),
	})

	test(new(testFilter))

	deleteAll(t, ds, input)
}

func SubtestReturnSizes(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, 100)
	subtestQuery(t, ktype, ds, dsq.Query{ReturnsSizes: true}, input)
	deleteAll(t, ds, input)
}

func SubtestPrefix(t *testing.T, ktype key.KeyType, ds dstore.Datastore) {
	input := prepareDs(t, ktype, ds, ElemCount)

	test := func(prefix string) {
		t.Run(prefix, func(t *testing.T) {
			subtestQuery(t, ktype, ds, dsq.Query{
				Prefix: key.QueryKeyFromTypeAndString(ktype, prefix),
			}, input)
		})
	}
	test("")
	test("/")
	test("/./")
	test("/.././/")
	test("/prefix/../")

	test("/prefix")
	test("/prefix/")
	test("/prefix/sub/")

	test("/0/")
	test("/bad/")

	deleteAll(t, ds, input)
}

func randValue() []byte {
	value := make([]byte, 64)
	rand.Read(value)
	return value
}

func prepareDs(t *testing.T, ktype key.KeyType, ds readWrite, count int) []dsq.Entry {
	var input []dsq.Entry
	ctx := context.Background()
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		key := key.NewKeyFromTypeAndString(ktype, s)
		value := randValue()
		input = append(input, dsq.Entry{
			Key:   key,
			Size:  len(value),
			Value: value,
		})
	}

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/prefix/%dkey%d", i, i)
		key := key.NewKeyFromTypeAndString(ktype, s)
		value := randValue()
		input = append(input, dsq.Entry{
			Key:   key,
			Size:  len(value),
			Value: value,
		})
	}

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/prefix/sub/%dkey%d", i, i)
		key := key.NewKeyFromTypeAndString(ktype, s)
		value := randValue()
		input = append(input, dsq.Entry{
			Key:   key,
			Size:  len(value),
			Value: value,
		})
	}

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/capital/%dKEY%d", i, i)
		key := key.NewKeyFromTypeAndString(ktype, s)
		value := randValue()
		input = append(input, dsq.Entry{
			Key:   key,
			Size:  len(value),
			Value: value,
		})
	}

	t.Logf("putting %d values", len(input))
	for i, e := range input {
		err := ds.Put(ctx, e.Key, e.Value)
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, e := range input {
		val, err := ds.Get(ctx, e.Key)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, e.Value) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}
	return input
}

// put many kvs, compare the results of ds with naiveApply
func subtestQuery(t *testing.T, ktype key.KeyType, ds dstore.Read, q dsq.Query, input []dsq.Entry) {
	// %dkey%d, /prefix/%dkey%d, /prefix/sub/%dkey%d, /capital/%dKEY%d
	ctx := context.Background()

	t.Log("querying values")
	resp, err := ds.Query(ctx, q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	if rq := resp.Query(); !reflect.DeepEqual(rq, q) {
		t.Errorf("returned query\n  %s\nexpected query\n  %s", &rq, q)
	}

	t.Log("aggregating query results")
	actual, err := resp.Rest()
	if err != nil {
		t.Fatal("query result error: ", err)
	}

	t.Log("verifying query output")
	expected, err := dsq.NaiveQueryApply(q, dsq.ResultsWithEntries(q, input)).Rest()
	if err != nil {
		t.Fatal("naive query error: ", err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(actual))
	}
	if len(q.Orders) == 0 {
		dsq.Sort([]dsq.Order{dsq.OrderByKey{}}, actual)
		dsq.Sort([]dsq.Order{dsq.OrderByKey{}}, expected)
	}
	for i := range actual {
		if !actual[i].Key.Equal(expected[i].Key) {
			t.Errorf("for result %d, expected key %q, got %q", i, expected[i].Key, actual[i].Key)
			continue
		}
		if !q.KeysOnly && !bytes.Equal(actual[i].Value, expected[i].Value) {
			t.Errorf("value mismatch for result %d (key=%q)", i, expected[i].Key)
		}
		if q.ReturnsSizes && actual[i].Size <= 0 {
			t.Errorf("for result %d, expected size > 0 with ReturnsSizes", i)
		}
	}
}

func deleteAll(t *testing.T, ds dstore.Write, input []dsq.Entry) {
	ctx := context.Background()

	t.Log("deleting all keys")
	for _, e := range input {
		if err := ds.Delete(ctx, e.Key); err != nil {
			t.Fatal(err)
		}
	}
}

func genQueryConditions(ktype key.KeyType) ([]int, []int, [][]dsq.Filter, []key.Key, [][]dsq.Order, []int) {
	offsets := []int{
		0,
		ElemCount / 10,
		ElemCount - 5,
		ElemCount,
	}
	limits := []int{
		0,
		1,
		ElemCount / 10,
		ElemCount,
	}
	filters := [][]dsq.Filter{
		{dsq.FilterKeyCompare{
			Op:  dsq.Equal,
			Key: key.QueryKeyFromTypeAndString(ktype, "/0key0"),
		}},
		{dsq.FilterKeyCompare{
			Op:  dsq.LessThan,
			Key: key.QueryKeyFromTypeAndString(ktype, "/2"),
		}},
	}
	prefixes := key.TypeAndStrsToQueryKeys(ktype, []string{
		"",
		"/prefix",
		"/0", // keys exist under this prefix but they shouldn't match.
	})
	orders := [][]dsq.Order{
		{dsq.OrderByKey{}},
		{dsq.OrderByKeyDescending{}},
		{dsq.OrderByValue{}, dsq.OrderByKey{}},
		{dsq.OrderByFunction(func(a, b dsq.Entry) int { return bytes.Compare(a.Value, b.Value) })},
	}
	lengths := []int{
		0,
		1,
		ElemCount,
	}
	return offsets, limits, filters, prefixes, orders, lengths
}
