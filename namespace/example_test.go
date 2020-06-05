package namespace_test

import (
	"fmt"

	ds "github.com/bdware/go-datastore"
	key "github.com/bdware/go-datastore/key"
	nsds "github.com/bdware/go-datastore/namespace"
)

func Example() {
	mp, _ := ds.NewMapDatastore(key.KeyTypeString)
	ns := nsds.Wrap(mp, key.NewStrKey("/foo/bar"))

	k := key.NewStrKey("/beep")
	v := "boop"

	if err := ns.Put(k, []byte(v)); err != nil {
		panic(err)
	}
	fmt.Printf("ns.Put %s %s\n", k, v)

	v2, _ := ns.Get(k)
	fmt.Printf("ns.Get %s -> %s\n", k, v2)

	k3 := key.NewStrKey("/foo/bar/beep")
	v3, _ := mp.Get(k3)
	fmt.Printf("mp.Get %s -> %s\n", k3, v3)
	// Output:
	// ns.Put /beep boop
	// ns.Get /beep -> boop
	// mp.Get /foo/bar/beep -> boop
}
