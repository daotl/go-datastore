package query

import (
	"testing"

	key "github.com/bdware/go-datastore/key"
)

func testKeyFilter(t *testing.T, f Filter, keys []key.Key, expect key.KeySlice) {
	t.Helper()
	e := make([]Entry, len(keys))
	for i, k := range keys {
		e[i] = Entry{Key: k}
	}

	res := ResultsWithEntries(Query{}, e)
	res = NaiveFilter(res, f)
	actualE, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}
	actual := key.KeySlice(make([]key.Key, len(actualE)))
	for i, e := range actualE {
		actual[i] = e.Key
	}

	if len(actual) != len(expect) {
		t.Error("expect != actual.", expect, actual)
	}

	if !actual.Join().Equal(expect.Join()) {
		t.Error("expect != actual.", expect, actual)
	}
}

func TestFilterKeyCompare(t *testing.T) {

	testKeyFilter(t, FilterKeyCompare{Equal, key.FilterStrKey("/ab")}, sampleKeys, key.StrsToKeys([]string{"/ab"}))
	testKeyFilter(t, FilterKeyCompare{GreaterThan, key.FilterStrKey("/ab")}, sampleKeys, key.StrsToKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/abce",
		"/abcf",
	}))
	testKeyFilter(t, FilterKeyCompare{LessThanOrEqual, key.FilterStrKey("/ab")}, sampleKeys, key.StrsToKeys([]string{
		"/a",
		"/ab",
	}))
}

func TestFilterKeyPrefix(t *testing.T) {

	testKeyFilter(t, FilterKeyPrefix{key.FilterStrKey("/a")}, sampleKeys, key.StrsToKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testKeyFilter(t, FilterKeyPrefix{key.FilterStrKey("/ab/")}, sampleKeys, key.StrsToKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
	}))
}
