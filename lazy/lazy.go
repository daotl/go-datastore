package lazy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

var (
	ErrClosed = errors.New("datastore closed")
)

// LazyDatastore wraps a datastore with three states: active, inactive and closed.
// LazyDatasotre is inited by four Actions init, activate, deactive and closeFunc.
// the relationship between states and Actions is as follows:
//
//                       -----------(deactivateFunc)----------
//                       |                                   |  -(ensureActive)-
//                       V                                   |  |              |
// nil -(initFunc)-> inactive -----(activateFunc)---------> active <------------
//                       |                                    |
//                       --(closeFunc)-> closed <-(closeFunc)--
//
// Caller can modify the child datastore in action, but caller is also
// responsible to make sure the child datastore is callable.
// Every call of lazyDatastore's datastore Interface will make sure datastore is
// in active state, otherwise an error will occur.

type LazyDatastore struct {
	child          datastore.Datastore
	rw             sync.RWMutex
	writing        bool
	active         bool
	closed         bool
	activateFunc   Action
	deactivateFunc Action
	closeFunc      Action
}

// Action is the lazyDatastore's state transfer function
type Action func(d *datastore.Datastore) error

func NewLazyDataStore(
	initFunc,
	activateFunc,
	deactivateFunc,
	closeFunc Action) (*LazyDatastore, error) {
	var d datastore.Datastore
	if initFunc == nil || activateFunc == nil || deactivateFunc == nil || closeFunc == nil {
		return nil, fmt.Errorf("nil modifier")
	}
	err := initFunc(&d)
	if err != nil {
		return nil, err
	}
	return &LazyDatastore{
		child:          d,
		rw:             sync.RWMutex{},
		writing:        false,
		active:         false,
		closed:         false,
		activateFunc:   activateFunc,
		deactivateFunc: deactivateFunc,
		closeFunc:      closeFunc,
	}, nil
}

// EnsureActive runs op and makes sure that the wrapped datastore.Datastore is in active state.
// EnsureActive returns any error if activation fails or op returns non-nil error.
func (l *LazyDatastore) EnsureActive(op Action) error {
	err := l.lockOrTransToActive()
	defer l.unlockActive()
	if err != nil {
		return err
	}
	return op(&l.child)
}

// Activate lazyDatastore.
// Activate doesn't ensure lazyDatastore keeping in active state.
// To keep in active state, use EnsureActive instead.
func (l *LazyDatastore) Activate() error {
	return l.EnsureActive(func(d *datastore.Datastore) error { return nil })
}

// Deactivate lazyDatastore.
func (l *LazyDatastore) Deactivate() error {
	l.rw.Lock()
	defer l.rw.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.active {
		return nil // already deactivate
	}
	err := l.deactivateFunc(&l.child)
	if err != nil {
		return err
	}
	l.active = false
	return nil
}

// Close the lazyDatastore.
// Return ErrClosed after being closed.
// Close will NOT called the child's closer.
// Close it in closeFunc.
func (l *LazyDatastore) Close() error {
	l.rw.Lock()
	defer l.rw.Unlock()
	if l.closed {
		return ErrClosed
	}
	err := l.closeFunc(&l.child)
	if err != nil {
		return err
	}
	l.closed = true
	return nil
}

// lockOrTransToActive checks if l is active and locks the datastore to active state.
// If l is active, prevent datastore's state being changed;
// If l is not active, change l.rw to exclusive(write) lock and activate it.
// Return any error while activating.
// It is caller's responsibility to make sure to call unlockActive in a same goroutine
// after calling lockOrTransToActive.
func (l *LazyDatastore) lockOrTransToActive() error {
	l.rw.RLock()
	if l.active {
		return nil
	}
	if l.closed {
		return ErrClosed
	}
	l.rw.RUnlock()
	l.rw.Lock() // Upgrade the lock if lazyDatastore is inactive.
	if l.active {
		return nil // double check to avoid calling activavteFunc repeatedly.
	}
	if l.closed {
		return ErrClosed
	}
	l.writing = true
	err := l.activateFunc(&l.child)
	if err != nil {
		return err
	}
	l.active = true
	return nil
}

// unlockActive releases datastore.
func (l *LazyDatastore) unlockActive() {
	if !l.writing {
		l.rw.RUnlock()
	} else {
		l.writing = false
		l.rw.Unlock()
	}
}

// Put implements Datastore.Put
func (l *LazyDatastore) Put(key key.Key, value []byte) (err error) {
	return l.EnsureActive(func(d *datastore.Datastore) error {
		return (*d).Put(key, value)
	})
}

// Sync implements Datastore.Sync
func (l *LazyDatastore) Sync(prefix key.Key) error {
	return l.EnsureActive(func(d *datastore.Datastore) error {
		return (*d).Sync(prefix)
	})
}

// Get implements Datastore.Get
func (l *LazyDatastore) Get(key key.Key) (value []byte, err error) {
	err = l.EnsureActive(func(d *datastore.Datastore) error {
		value, err = (*d).Get(key)
		return err
	})
	return value, err
}

// Has implements Datastore.Has
func (l *LazyDatastore) Has(key key.Key) (exists bool, err error) {
	err = l.EnsureActive(func(d *datastore.Datastore) error {
		exists, err = (*d).Has(key)
		return err
	})
	return exists, err
}

// GetSize implements Datastore.GetSize
func (l *LazyDatastore) GetSize(key key.Key) (size int, err error) {
	err = l.EnsureActive(func(d *datastore.Datastore) error {
		size, err = (*d).GetSize(key)
		return err
	})
	return size, err
}

// Delete implements Datastore.Delete
func (l *LazyDatastore) Delete(key key.Key) (err error) {
	return l.EnsureActive(func(d *datastore.Datastore) error {
		return (*d).Delete(key)
	})
}

// Query implements Datastore.Query
func (l *LazyDatastore) Query(q query.Query) (rs query.Results, err error) {
	err = l.EnsureActive(func(d *datastore.Datastore) error {
		rs, err = (*d).Query(q)
		return err
	})
	return rs, err
}
