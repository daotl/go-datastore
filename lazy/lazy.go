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

type DSModifier func(d *datastore.Datastore) error

// LazyDatastore wraps a datastore with three states: active, inactive and closed.
// LazyDatasotre is inited by four DSModifier: init, activate, deactive and closeFunc.
// the relationship between states and DSModifiers is as follows:
//
//                       -----------(deactivateFunc)----------
//                       |                                   |
//                       V                                   |
// nil -(initFunc)-> deactive -----(activateFunc)---------> active
//                       |                                    |
//                       --(closeFunc)-> closed <-(closeFunc)--
//
// Every call of lazyDatastore will make sure datastore is active, otherwise an
// error will occur.

type LazyDatastore struct {
	child          datastore.Datastore
	rw             sync.RWMutex
	writing        bool
	active         bool
	closed         bool
	activateFunc   DSModifier
	deactivateFunc DSModifier
	closeFunc      DSModifier
}

func NewLazyDataStore(
	initFunc,
	activateFunc,
	deactivateFunc,
	closeFunc DSModifier) (*LazyDatastore, error) {
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

// LockActive check if l is opened and lock the datastore to active state.
// If l is opened, prevent datastore's state being changed;
// If l is not opened, change l.rw to exclusive(write) lock and open it.
// Return any error when opening the datastore.
func (l *LazyDatastore) LockActive() error {
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

// UnlockActive releases datastore.
func (l *LazyDatastore) UnlockActive() {
	if !l.writing {
		l.rw.RUnlock()
	} else {
		l.writing = false
		l.rw.Unlock()
	}
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
func (l *LazyDatastore) Close() error {
	l.rw.Lock()
	defer l.rw.Unlock()
	err := l.closeFunc(&l.child)
	if err != nil {
		return err
	}
	l.closed = true
	return nil
}

// Put implements Datastore.Put
func (l *LazyDatastore) Put(key key.Key, value []byte) (err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return err
	}
	return l.child.Put(key, value)
}

// Sync implements Datastore.Sync
func (l *LazyDatastore) Sync(prefix key.Key) error {
	err := l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return err
	}
	return l.child.Sync(prefix)
}

// Get implements Datastore.Get
func (l *LazyDatastore) Get(key key.Key) (value []byte, err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return []byte{}, err
	}
	return l.child.Get(key)
}

// Has implements Datastore.Has
func (l *LazyDatastore) Has(key key.Key) (exists bool, err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return false, err
	}
	return l.child.Has(key)
}

// GetSize implements Datastore.GetSize
func (l *LazyDatastore) GetSize(key key.Key) (size int, err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return 0, err
	}
	return l.child.GetSize(key)
}

// Delete implements Datastore.Delete
func (l *LazyDatastore) Delete(key key.Key) (err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return err
	}
	return l.child.Delete(key)
}

// Query implements Datastore.Query
func (l *LazyDatastore) Query(q query.Query) (rs query.Results, err error) {
	err = l.LockActive()
	defer l.UnlockActive()
	if err != nil {
		return nil, err
	}
	return l.child.Query(q)
}
