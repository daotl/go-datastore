package datastore

import key "github.com/bdware/go-datastore/key"

type op struct {
	delete bool
	value  []byte
}

// basicBatch implements the transaction interface for datastores who do
// not have any sort of underlying transactional support
type basicBatch struct {
	ops map[key.Key]op

	target Datastore
}

func NewBasicBatch(ds Datastore) Batch {
	return &basicBatch{
		ops:    make(map[key.Key]op),
		target: ds,
	}
}

func (bt *basicBatch) Put(key key.Key, val []byte) error {
	bt.ops[key] = op{value: val}
	return nil
}

func (bt *basicBatch) Delete(key key.Key) error {
	bt.ops[key] = op{delete: true}
	return nil
}

func (bt *basicBatch) Commit() error {
	var err error
	for k, op := range bt.ops {
		if op.delete {
			err = bt.target.Delete(k)
		} else {
			err = bt.target.Put(k, op.value)
		}
		if err != nil {
			break
		}
	}

	return err
}
