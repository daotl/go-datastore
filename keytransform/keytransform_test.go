// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package keytransform_test

import (
	"bytes"
	"sort"
	"testing"

	. "gopkg.in/check.v1"

	ds "github.com/bdware/go-datastore"
	key "github.com/bdware/go-datastore/key"
	kt "github.com/bdware/go-datastore/keytransform"
	dsq "github.com/bdware/go-datastore/query"
	dstest "github.com/bdware/go-datastore/test"
)

// Hook up gocheck into the "go test" runner.
func TestStrKey(t *testing.T) { TestingT(t) }

type DSSuite struct{}

var _ = Suite(&DSSuite{})

var pair = &kt.Pair{
	Convert: func(k key.Key) key.Key {
		return key.NewStrKey("/abc").Child(k)
	},
	Invert: func(k key.Key) key.Key {
		// remove abc prefix
		l := k.(key.StrKey).List()
		if l[0] != "abc" {
			panic("key does not have prefix. convert failed?")
		}
		return key.KeyWithNamespaces(l[1:])
	},
}

func (ks *DSSuite) TestStrKeyBasic(c *C) {
	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, pair)

	keys := key.StrsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ktds.Put(k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v1, err := ktds.Get(k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, []byte(k.String())), Equals, true)

		v2, err := mpds.Get(key.NewStrKey("abc").Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v2, []byte(k.String())), Equals, true)
	}

	run := func(d ds.Datastore, q dsq.Query) []key.Key {
		r, err := d.Query(q)
		c.Check(err, Equals, nil)

		e, err := r.Rest()
		c.Check(err, Equals, nil)

		return dsq.EntryKeys(e)
	}

	listA := run(mpds, dsq.Query{})
	listB := run(ktds, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))

	// sort them cause yeah.
	sort.Sort(key.KeySlice(listA))
	sort.Sort(key.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		c.Check(pair.Invert(kA), Equals, kB)
		c.Check(kA, Equals, pair.Convert(kB))
	}

	c.Log("listA: ", listA)
	c.Log("listB: ", listB)

	if err := ktds.Check(); err != dstest.TestError {
		c.Errorf("Unexpected Check() error: %s", err)
	}

	if err := ktds.CollectGarbage(); err != dstest.TestError {
		c.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := ktds.Scrub(); err != dstest.TestError {
		c.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestSuiteStrKeyDefaultPair(t *testing.T) {
	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, pair)
	dstest.SubtestAll(t, ktds)
}

func TestSuiteStrKeyPrefixTransform(t *testing.T) {
	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, kt.PrefixTransform{Prefix: key.NewStrKey("/foo")})
	dstest.SubtestAll(t, ktds)
}
