package mount_test

import (
	"errors"
	"testing"

	datastore "github.com/bdware/go-datastore"
	autobatch "github.com/bdware/go-datastore/autobatch"
	key "github.com/bdware/go-datastore/key"
	mount "github.com/bdware/go-datastore/mount"
	query "github.com/bdware/go-datastore/query"
	sync "github.com/bdware/go-datastore/sync"
	dstest "github.com/bdware/go-datastore/test"
)

func TestPutBadNothing(t *testing.T) {
	m := mount.New(nil)

	err := m.Put(key.NewStrKey("quux"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("Put got wrong error: %v != %v", g, e)
	}
}

func TestPutBadNoMount(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/redherring"), Datastore: mapds},
	})

	err := m.Put(key.NewStrKey("/quux/thud"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("expected ErrNoMount, got: %v\n", g)
	}
}

func TestPut(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	if err := m.Put(key.NewStrKey("/quux/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	buf, err := mapds.Get(key.NewStrKey("/thud"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestGetBadNothing(t *testing.T) {
	m := mount.New([]mount.Mount{})

	_, err := m.Get(key.NewStrKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNoMount(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/redherring"), Datastore: mapds},
	})

	_, err := m.Get(key.NewStrKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFound(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	_, err := m.Get(key.NewStrKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGet(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(key.NewStrKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	buf, err := m.Get(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestHasBadNothing(t *testing.T) {
	m := mount.New([]mount.Mount{})

	found, err := m.Has(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasBadNoMount(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/redherring"), Datastore: mapds},
	})

	found, err := m.Has(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasNotFound(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	found, err := m.Has(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHas(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(key.NewStrKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	found, err := m.Has(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestDeleteNotFound(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	err := m.Delete(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("expected nil, got: %v\n", err)
	}
}

func TestDelete(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(key.NewStrKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := m.Delete(key.NewStrKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// make sure it disappeared
	found, err := mapds.Has(key.NewStrKey("/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestQuerySimple(t *testing.T) {
	mapds := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/quux"), Datastore: mapds},
	})

	var myKey = key.NewStrKey("/quux/thud")
	if err := m.Put(myKey, []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	res, err := m.Query(query.Query{Prefix: "/quux"})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := false
	for _, e := range entries {
		if e.Key.Equal(myKey) {
			seen = true
		} else {
			t.Errorf("saw unexpected key: %q", e.Key)
		}
	}
	if !seen {
		t.Errorf("did not see wanted key %q in %+v", myKey, entries)
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryAcrossMounts(t *testing.T) {
	mapds0 := dstest.NewMapDatastoreForTest(t)
	mapds1 := dstest.NewMapDatastoreForTest(t)
	mapds2 := dstest.NewMapDatastoreForTest(t)
	mapds3 := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/foo"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/bar"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/baz"), Datastore: mapds3},
		{Prefix: key.NewStrKey("/"), Datastore: mapds0},
	})

	if err := m.Put(key.NewStrKey("/foo/lorem"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/bar/ipsum"), []byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/bar/dolor"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/baz/sit"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/banana"), []byte("567")); err != nil {
		t.Fatal(err)
	}

	expect := func(prefix string, values map[string]string) {
		t.Helper()
		res, err := m.Query(query.Query{Prefix: prefix})
		if err != nil {
			t.Fatalf("Query fail: %v\n", err)
		}
		entries, err := res.Rest()
		if err != nil {
			err = res.Close()
			if err != nil {
				t.Errorf("result.Close failed %d", err)
			}
			t.Fatalf("Query Results.Rest fail: %v\n", err)
		}
		if len(entries) != len(values) {
			t.Errorf("expected %d results, got %d", len(values), len(entries))
		}
		for _, e := range entries {
			v, ok := values[e.Key.String()]
			if !ok {
				t.Errorf("unexpected key %s", e.Key)
				continue
			}

			if v != string(e.Value) {
				t.Errorf("key value didn't match expected %s: '%s' - '%s'", e.Key, v, e.Value)
			}

			values[e.Key.String()] = "seen"
		}
	}

	expect("/ba", nil)
	expect("/bar", map[string]string{
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
	})
	expect("/baz/", map[string]string{
		"/baz/sit": "456",
	})
	expect("/foo", map[string]string{
		"/foo/lorem": "123",
	})
	expect("/", map[string]string{
		"/foo/lorem": "123",
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
		"/baz/sit":   "456",
		"/banana":    "567",
	})
	expect("/banana", nil)
}

func TestQueryAcrossMountsWithSort(t *testing.T) {
	mapds0 := dstest.NewMapDatastoreForTest(t)
	mapds1 := dstest.NewMapDatastoreForTest(t)
	mapds2 := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/boo/5"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/boo"), Datastore: mapds0},
	})

	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/1"), []byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/boo/9"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/boo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/boo/5/hello"), []byte("789")); err != nil {
		t.Fatal(err)
	}

	res, err := m.Query(query.Query{Orders: []query.Order{query.OrderByKey{}}})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToKeys([]string{
		"/boo/3",
		"/boo/5/hello",
		"/boo/9",
		"/zoo/0",
		"/zoo/1",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAcrossMountsWithSort(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(key.NewStrKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 2, Orders: []query.Order{query.OrderByKeyDescending{}}}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToKeys([]string{
		"/zoo/3",
		"/zoo/2",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetAcrossMountsWithSort(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(key.NewStrKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 3, Offset: 2, Orders: []query.Order{query.OrderByKey{}}}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToKeys([]string{
		"/rok/3",
		"/zoo/0",
		"/zoo/1",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryFilterAcrossMountsWithSort(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(key.NewStrKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	f := &query.FilterKeyCompare{Op: query.Equal, Key: key.QueryStrKey("/rok/3")}
	q := query.Query{Filters: []query.Filter{f}}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToKeys([]string{
		"/rok/3",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetWithNoData(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
	})

	q := query.Query{Limit: 4, Offset: 3}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitWithNotEnoughData(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
	})

	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 4}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/zoo/0",
		"/rok/1",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryOffsetWithNotEnoughData(t *testing.T) {
	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/rok"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/zoo"), Datastore: mapds2},
	})

	if err := m.Put(key.NewStrKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Offset: 4}
	res, err := m.Query(q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestLookupPrio(t *testing.T) {
	mapds0 := dstest.NewMapDatastoreForTest(t)
	mapds1 := dstest.NewMapDatastoreForTest(t)

	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/"), Datastore: mapds0},
		{Prefix: key.NewStrKey("/foo"), Datastore: mapds1},
	})

	if err := m.Put(key.NewStrKey("/foo/bar"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key.NewStrKey("/baz"), []byte("234")); err != nil {
		t.Fatal(err)
	}

	found, err := mapds0.Has(key.NewStrKey("/baz"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds0.Has(key.NewStrKey("/foo/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds1.Has(key.NewStrKey("/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestNestedMountSync(t *testing.T) {
	internalDSRoot := dstest.NewMapDatastoreForTest(t)
	internalDSFoo := dstest.NewMapDatastoreForTest(t)
	internalDSFooBar := dstest.NewMapDatastoreForTest(t)

	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/foo"), Datastore: autobatch.NewAutoBatching(internalDSFoo, 10)},
		{Prefix: key.NewStrKey("/foo/bar"), Datastore: autobatch.NewAutoBatching(internalDSFooBar, 10)},
		{Prefix: key.NewStrKey("/"), Datastore: autobatch.NewAutoBatching(internalDSRoot, 10)},
	})

	// Testing scenarios
	// 1) Make sure child(ren) sync
	// 2) Make sure parent syncs
	// 3) Make sure parent only syncs the relevant subtree (instead of fully syncing)

	addToDS := func(str string) {
		t.Helper()
		if err := m.Put(key.NewStrKey(str), []byte(str)); err != nil {
			t.Fatal(err)
		}
	}

	checkVal := func(d datastore.Datastore, str string, expectFound bool) {
		t.Helper()
		res, err := d.Has(key.NewStrKey(str))
		if err != nil {
			t.Fatal(err)
		}
		if res != expectFound {
			if expectFound {
				t.Fatal("datastore is missing key")
			}
			t.Fatal("datastore has key it should not have")
		}
	}

	// Add /foo/bar/0, Add /foo/bar/0/1, Add /foo/baz, Add /beep/bop, Sync /foo: all added except last - checks 1 and 2
	addToDS("/foo/bar/0")
	addToDS("/foo/bar/1")
	addToDS("/foo/baz")
	addToDS("/beep/bop")

	if err := m.Sync(key.NewStrKey("/foo")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSFooBar, "/0", true)
	checkVal(internalDSFooBar, "/1", true)
	checkVal(internalDSFoo, "/baz", true)
	checkVal(internalDSRoot, "/beep/bop", false)

	// Add /fwop Add /bloop Sync /fwop, both added - checks 3
	addToDS("/fwop")
	addToDS("/bloop")

	if err := m.Sync(key.NewStrKey("/fwop")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSRoot, "/fwop", true)
	checkVal(internalDSRoot, "/bloop", false)
}

type errQueryDS struct {
	datastore.NullDatastore
}

func (d *errQueryDS) Query(q query.Query) (query.Results, error) {
	return nil, errors.New("test error")
}

func TestErrQueryClose(t *testing.T) {
	eqds := &errQueryDS{}
	mds := dstest.NewMapDatastoreForTest(t)

	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/"), Datastore: mds},
		{Prefix: key.NewStrKey("/foo"), Datastore: eqds},
	})

	if err := m.Put(key.NewStrKey("/baz"), []byte("123")); err != nil {
		t.Fatal(err)
	}

	_, err := m.Query(query.Query{})
	if err == nil {
		t.Fatal("expected query to fail")
		return
	}
}

func TestMaintenanceFunctions(t *testing.T) {
	mapds := dstest.NewTestDatastore(true)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/"), Datastore: mapds},
	})

	if err := m.Check(); err.Error() != "checking datastore at /: test error" {
		t.Errorf("Unexpected Check() error: %s", err)
	}

	if err := m.CollectGarbage(); err.Error() != "gc on datastore at /: test error" {
		t.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := m.Scrub(); err.Error() != "scrubbing datastore at /: test error" {
		t.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestSuite(t *testing.T) {
	mapds0 := dstest.NewMapDatastoreForTest(t)
	mapds1 := dstest.NewMapDatastoreForTest(t)
	mapds2 := dstest.NewMapDatastoreForTest(t)
	mapds3 := dstest.NewMapDatastoreForTest(t)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewStrKey("/prefix"), Datastore: mapds1},
		{Prefix: key.NewStrKey("/prefix/sub"), Datastore: mapds2},
		{Prefix: key.NewStrKey("/0"), Datastore: mapds3},
		{Prefix: key.NewStrKey("/"), Datastore: mapds0},
	})
	dstest.SubtestAll(t, m)
}
