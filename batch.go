package datastore

import key "github.com/bdware/go-datastore/key"

type op struct {
	key    key.Key
	delete bool
	value  []byte
}

// basicBatch implements the transaction interface for datastores who do
// not have any sort of underlying transactional support
type basicBatch struct {
	ops map[string]op

	target Datastore
}

func NewBasicBatch(ds Datastore) Batch {
	return &basicBatch{
		ops:    make(map[string]op),
		target: ds,
	}
}

func (bt *basicBatch) Put(key key.Key, val []byte) error {
	bt.ops[key.String()] = op{key: key, value: val}
	return nil
}

func (bt *basicBatch) Delete(key key.Key) error {
	bt.ops[key.String()] = op{key: key, delete: true}
	return nil
}

func (bt *basicBatch) Commit() error {
	var err error
	for _, op := range bt.ops {
		if op.delete {
			err = bt.target.Delete(op.key)
		} else {
			err = bt.target.Put(op.key, op.value)
		}
		if err != nil {
			break
		}
	}

	return err
}
