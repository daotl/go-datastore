// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package key_test

import (
	"bytes"
	"path"
	"strings"
	"testing"

	. "gopkg.in/check.v1"

	. "github.com/bdware/go-datastore/key"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type StrKeySuite struct{}

var _ = Suite(&StrKeySuite{})

func (ks *StrKeySuite) SubtestKey(s string, c *C) {
	fixed := path.Clean("/" + s)
	namespaces := strings.Split(fixed, "/")[1:]
	lastNamespace := namespaces[len(namespaces)-1]
	lnparts := strings.Split(lastNamespace, ":")
	ktype := ""
	if len(lnparts) > 1 {
		ktype = strings.Join(lnparts[:len(lnparts)-1], ":")
	}
	kname := lnparts[len(lnparts)-1]

	kchild := path.Clean(fixed + "/cchildd")
	kparent := "/" + strings.Join(namespaces[:len(namespaces)-1], "/")
	kpath := path.Clean(kparent + "/" + ktype)
	kinstance := fixed + ":" + "inst"

	c.Log("Testing: ", NewStrKey(s))

	c.Check(NewStrKey(s).String(), Equals, fixed)
	c.Check(NewStrKey(s), Equals, NewStrKey(s))
	c.Check(NewStrKey(s).String(), Equals, NewStrKey(s).String())
	c.Check(NewStrKey(s).Name(), Equals, kname)
	c.Check(NewStrKey(s).Type(), Equals, ktype)
	c.Check(NewStrKey(s).Path().String(), Equals, kpath)
	c.Check(NewStrKey(s).Instance("inst").String(), Equals, kinstance)

	c.Check(NewStrKey(s).Child(NewStrKey("cchildd")).String(), Equals, kchild)
	c.Check(NewStrKey(s).Child(NewStrKey("cchildd")).Parent().String(), Equals, fixed)
	c.Check(NewStrKey(s).ChildString("cchildd").String(), Equals, kchild)
	c.Check(NewStrKey(s).ChildString("cchildd").Parent().String(), Equals, fixed)
	c.Check(NewStrKey(s).Parent().String(), Equals, kparent)
	c.Check(len(NewStrKey(s).List()), Equals, len(namespaces))
	c.Check(len(NewStrKey(s).Namespaces()), Equals, len(namespaces))
	for i, e := range NewStrKey(s).List() {
		c.Check(namespaces[i], Equals, e)
	}

	c.Check(NewStrKey(s), Equals, NewStrKey(s))
	c.Check(NewStrKey(s).Equal(NewStrKey(s)), Equals, true)
	c.Check(NewStrKey(s).Equal(NewStrKey("/fdsafdsa/"+s)), Equals, false)

	// less
	c.Check(NewStrKey(s).Less(NewStrKey(s).Parent()), Equals, false)
	c.Check(NewStrKey(s).Less(NewStrKey(s).ChildString("foo")), Equals, true)
}

func (ks *StrKeySuite) TestKeyBasic(c *C) {
	ks.SubtestKey("", c)
	ks.SubtestKey("abcde", c)
	ks.SubtestKey("disahfidsalfhduisaufidsail", c)
	ks.SubtestKey("/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/", c)
	ks.SubtestKey("4215432143214321432143214321", c)
	ks.SubtestKey("/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/", c)
	ks.SubtestKey("abcde:fdsfd", c)
	ks.SubtestKey("disahfidsalfhduisaufidsail:fdsa", c)
	ks.SubtestKey("/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/:", c)
	ks.SubtestKey("4215432143214321432143214321:", c)
	ks.SubtestKey("fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/f:fdaf", c)
}

func CheckTrue(c *C, cond bool) {
	c.Check(cond, Equals, true)
}

func (ks *StrKeySuite) TestKeyAncestry(c *C) {
	k1 := NewStrKey("/A/B/C")
	k2 := NewStrKey("/A/B/C/D")
	k3 := NewStrKey("/AB")
	k4 := NewStrKey("/A")

	c.Check(k1.String(), Equals, "/A/B/C")
	c.Check(k2.String(), Equals, "/A/B/C/D")
	CheckTrue(c, k1.IsAncestorOf(k2))
	CheckTrue(c, k2.IsDescendantOf(k1))
	CheckTrue(c, k4.IsAncestorOf(k2))
	CheckTrue(c, k4.IsAncestorOf(k1))
	CheckTrue(c, !k4.IsDescendantOf(k2))
	CheckTrue(c, !k4.IsDescendantOf(k1))
	CheckTrue(c, !k3.IsDescendantOf(k4))
	CheckTrue(c, !k4.IsAncestorOf(k3))
	CheckTrue(c, k2.IsDescendantOf(k4))
	CheckTrue(c, k1.IsDescendantOf(k4))
	CheckTrue(c, !k2.IsAncestorOf(k4))
	CheckTrue(c, !k1.IsAncestorOf(k4))
	CheckTrue(c, !k2.IsAncestorOf(k2))
	CheckTrue(c, !k1.IsAncestorOf(k1))
	c.Check(k1.Child(NewStrKey("D")).String(), Equals, k2.String())
	c.Check(k1.ChildString("D").String(), Equals, k2.String())
	c.Check(k1.String(), Equals, k2.Parent().String())
	c.Check(k1.Path().String(), Equals, k2.Parent().Path().String())
}

func (ks *StrKeySuite) TestType(c *C) {
	k1 := NewStrKey("/A/B/C:c")
	k2 := NewStrKey("/A/B/C:c/D:d")

	CheckTrue(c, k1.IsAncestorOf(k2))
	CheckTrue(c, k2.IsDescendantOf(k1))
	c.Check(k1.Type(), Equals, "C")
	c.Check(k2.Type(), Equals, "D")
	c.Check(k1.Type(), Equals, k2.Parent().Type())
}

func (ks *StrKeySuite) TestRandom(c *C) {
	keys := map[Key]bool{}
	for i := 0; i < 1000; i++ {
		r := RandomStrKey()
		_, found := keys[r]
		CheckTrue(c, !found)
		keys[r] = true
	}
	CheckTrue(c, len(keys) == 1000)
}

func (ks *StrKeySuite) TestLess(c *C) {

	checkLess := func(a, b string) {
		ak := NewStrKey(a)
		bk := NewStrKey(b)
		c.Check(ak.Less(bk), Equals, true)
		c.Check(bk.Less(ak), Equals, false)
	}

	checkLess("/a/b/c", "/a/b/c/d")
	checkLess("/a/b", "/a/b/c/d")
	checkLess("/a", "/a/b/c/d")
	checkLess("/a/a/c", "/a/b/c")
	checkLess("/a/a/d", "/a/b/c")
	checkLess("/a/b/c/d/e/f/g/h", "/b")
	checkLess("/", "/a")
}

func TestStrKeyMarshalJSON(t *testing.T) {
	cases := []struct {
		key  Key
		data []byte
		err  string
	}{
		{NewStrKey("/a/b/c"), []byte("\"/a/b/c\""), ""},
		{NewStrKey("/shouldescapekey\"/with/quote"), []byte("\"/shouldescapekey\\\"/with/quote\""), ""},
	}

	for i, c := range cases {
		out, err := c.key.MarshalJSON()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d marshal error mismatch: expected: %s, got: %s", i, c.err, err)
		}
		if !bytes.Equal(c.data, out) {
			t.Errorf("case %d value mismatch: expected: %s, got: %s", i, string(c.data), string(out))
		}

		if c.err == "" {
			key := NewStrKey("").(StrKey)
			if err := key.UnmarshalJSON(out); err != nil {
				t.Errorf("case %d error parsing key from json output: %s", i, err.Error())
			}
			if !c.key.Equal(key) {
				t.Errorf("case %d parsed key from json output mismatch. expected: %s, got: %s", i, c.key.String(), key.String())
			}
		}
	}
}

func TestStrKeyUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data []byte
		key  Key
		err  string
	}{
		{[]byte("\"/a/b/c\""), NewStrKey("/a/b/c"), ""},
		{[]byte{}, NewStrKey(""), "unexpected end of JSON input"},
		{[]byte{'"'}, NewStrKey(""), "unexpected end of JSON input"},
		{[]byte(`""`), NewStrKey(""), ""},
	}

	for i, c := range cases {
		key := NewStrKey("").(StrKey)
		err := key.UnmarshalJSON(c.data)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d marshal error mismatch: expected: %s, got: %s", i, c.err, err)
		}

		if !key.Equal(c.key) {
			t.Errorf("case %d key mismatch: expected: %s, got: %s", i, c.key, key)
		}
	}
}
