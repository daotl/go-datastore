package fuzzer

import (
	ds "github.com/bdware/go-datastore"
	prov "github.com/bdware/go-ds-flatfs"
)

func init() {
	AddOpener("go-ds-flatfs", func(loc string) ds.Datastore {
		d, err := prov.CreateOrOpen(loc, prov.IPFS_DEF_SHARD, false)
		if err != nil {
			panic("could not create db instance")
		}
		return d
	})
}
