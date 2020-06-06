// Package mount provides a Datastore that has other Datastores
// mounted at various key prefixes and is threadsafe
package mount

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"go.uber.org/multierr"

	ds "github.com/bdware/go-datastore"
	key "github.com/bdware/go-datastore/key"
	query "github.com/bdware/go-datastore/query"
)

var (
	ErrNoMount = errors.New("no datastore mounted for this key")
)

// Mount defines a datastore mount. It mounts the given datastore at the given
// prefix.
type Mount struct {
	Prefix    key.Key
	Datastore ds.Datastore
}

// New creates a new mount datstore from the given mounts. See the documentation
// on Datastore for details.
//
// The order of the mounts does not matter, they will be applied most specific
// to least specific.
func New(mounts []Mount) *Datastore {
	// make a copy so we're sure it doesn't mutate
	m := make([]Mount, len(mounts))
	copy(m, mounts)
	sort.Slice(m, func(i, j int) bool { return m[i].Prefix.String() > m[j].Prefix.String() })
	return &Datastore{mounts: m}
}

// Datastore is a mount datastore. In this datastore, keys live under the most
// specific mounted sub-datastore. That is, given sub-datastores mounted under:
//
// * /
// * /foo
// * /foo/bar
//
// Keys would be written as follows:
//
// * /foo, /foobar, /baz would all live under /.
// * /foo/baz, /foo/bar, etc. would live under /foo.
// * /foo/bar/baz would live under /foo/bar.
//
// Additionally, even if the datastore mounted at / contains the key /foo/thing,
// the datastore mounted at /foo would mask this value in get, deletes, and
// query results.
//
// Finally, if no root (/) mount is provided, operations on keys living outside
// all of the provided mounts will behave as follows:
//
// * Get - Returns datastore.ErrNotFound.
// * Query - Returns no results.
// * Put - Returns ErrNoMount.
type Datastore struct {
	mounts []Mount
}

var _ ds.Datastore = (*Datastore)(nil)

// lookup looks up the datastore in which the given key lives.
func (d *Datastore) lookup(k key.Key) (ds.Datastore, key.Key, key.Key) {
	for _, m := range d.mounts {
		if m.Prefix.IsAncestorOf(k) {
			s := strings.TrimPrefix(k.String(), m.Prefix.String())
			found := key.NewStrKey(s)
			return m.Datastore, m.Prefix, found
		}
	}
	return nil, key.NewStrKey("/"), k
}

type queryResults struct {
	mount   key.Key
	results query.Results
	next    query.Result
}

func (qr *queryResults) advance() bool {
	if qr.results == nil {
		return false
	}

	qr.next = query.Result{}
	r, more := qr.results.NextSync()
	if !more {
		err := qr.results.Close()
		qr.results = nil
		if err != nil {
			// One more result, the error.
			qr.next = query.Result{Error: err}
			return true
		}
		return false
	}

	r.Key = qr.mount.Child(r.Key)
	qr.next = r
	return true
}

type querySet struct {
	query query.Query
	heads []*queryResults
}

func (h *querySet) Len() int {
	return len(h.heads)
}

func (h *querySet) Less(i, j int) bool {
	return query.Less(h.query.Orders, h.heads[i].next.Entry, h.heads[j].next.Entry)
}

func (h *querySet) Swap(i, j int) {
	h.heads[i], h.heads[j] = h.heads[j], h.heads[i]
}

func (h *querySet) Push(x interface{}) {
	h.heads = append(h.heads, x.(*queryResults))
}

func (h *querySet) Pop() interface{} {
	i := len(h.heads) - 1
	last := h.heads[i]
	h.heads[i] = nil
	h.heads = h.heads[:i]
	return last
}

func (h *querySet) close() error {
	var errs []error
	for _, qr := range h.heads {
		err := qr.results.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	h.heads = nil
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (h *querySet) addResults(mount key.Key, results query.Results) {
	r := &queryResults{
		results: results,
		mount:   mount,
	}
	if r.advance() {
		heap.Push(h, r)
	}
}

func (h *querySet) next() (query.Result, bool) {
	if len(h.heads) == 0 {
		return query.Result{}, false
	}
	head := h.heads[0]
	next := head.next

	if head.advance() {
		heap.Fix(h, 0)
	} else {
		heap.Remove(h, 0)
	}

	return next, true
}

// lookupAll returns all mounts that might contain keys that are strict
// descendants of <key>. It will not return mounts that match key exactly.
//
// Specifically, this function will return three slices:
//
// * The matching datastores.
// * The prefixes where each matching datastore has been mounted.
// * The prefix within these datastores at which descendants of the passed key
//   live. If the mounted datastore is fully contained within the given key,
//   this will be /.
//
// By example, given the datastores:
//
// * /         - root
// * /foo      -
// * /bar
// * /foo/bar
//
// This function function will behave as follows:
//
// * key       -> ([mountpoints], [rests])                  # comment
// * /         -> ([/, /foo, /bar, /foo/bar], [/, /, /, /]) # all datastores
// * /foo      -> ([/foo, /foo/bar], [/, /])                # all datastores under /foo
// * /foo/bar  -> ([/foo/bar], [/])                         # /foo/bar
// * /bar/foo  -> ([/bar], [/foo])                          # the datastore mounted at /bar, rest is /foo
// * /ba       -> ([/], [/])                                # the root; only full components are matched.
func (d *Datastore) lookupAll(k key.Key) (dst []ds.Datastore, mountpoint, rest []key.Key) {
	for _, m := range d.mounts {
		if m.Prefix.IsDescendantOf(k) {
			dst = append(dst, m.Datastore)
			mountpoint = append(mountpoint, m.Prefix)
			rest = append(rest, key.NewStrKey("/"))
		} else if m.Prefix.Equal(k) || m.Prefix.IsAncestorOf(k) {
			r := strings.TrimPrefix(k.String(), m.Prefix.String())

			dst = append(dst, m.Datastore)
			mountpoint = append(mountpoint, m.Prefix)
			rest = append(rest, key.NewStrKey(r))

			// We've found an ancestor (or equal) key. We might have
			// more general datastores, but they won't contain keys
			// with this prefix so there's no point in searching them.
			break
		}
	}
	return dst, mountpoint, rest
}

// Put puts the given value into the datastore at the given key.
//
// Returns ErrNoMount if there no datastores are mounted at the appropriate
// prefix for the given key.
func (d *Datastore) Put(key key.Key, value []byte) error {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return ErrNoMount
	}
	return cds.Put(k, value)
}

// Sync implements Datastore.Sync
func (d *Datastore) Sync(prefix key.Key) error {
	var merr error

	// Sync all mount points below the prefix
	// Sync the mount point right at (or above) the prefix
	dstores, prefixes, rest := d.lookupAll(prefix)
	for i, suffix := range rest {
		if err := dstores[i].Sync(suffix); err != nil {
			merr = multierr.Append(merr, fmt.Errorf(
				"syncing datastore at %s: %w",
				prefixes[i].String(),
				err,
			))
		}
	}

	return merr
}

// Get returns the value associated with the key from the appropriate datastore.
func (d *Datastore) Get(key key.Key) (value []byte, err error) {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return nil, ds.ErrNotFound
	}
	return cds.Get(k)
}

// Has returns the true if there exists a value associated with key in the
// appropriate datastore.
func (d *Datastore) Has(key key.Key) (exists bool, err error) {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return false, nil
	}
	return cds.Has(k)
}

// Get returns the size of the value associated with the key in the appropriate
// datastore.
func (d *Datastore) GetSize(key key.Key) (size int, err error) {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return -1, ds.ErrNotFound
	}
	return cds.GetSize(k)
}

// Delete deletes the value associated with the key in the appropriate
// datastore.
//
// Delete returns no error if there is no value associated with the given key.
func (d *Datastore) Delete(key key.Key) error {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return nil
	}
	return cds.Delete(k)
}

// Query queries the appropriate mounted datastores, merging the results
// according to the given orders.
//
// If a query prefix is specified, Query will avoid querying datastores mounted
// outside that prefix.
func (d *Datastore) Query(master query.Query) (query.Results, error) {
	childQuery := query.Query{
		Prefix:            master.Prefix,
		Orders:            master.Orders,
		KeysOnly:          master.KeysOnly,
		ReturnExpirations: master.ReturnExpirations,
		ReturnsSizes:      master.ReturnsSizes,
	}

	dses, mounts, rests := d.lookupAll(key.Clean(childQuery.Prefix))

	queries := &querySet{
		query: childQuery,
		heads: make([]*queryResults, 0, len(dses)),
	}

	for i := range dses {
		mount := mounts[i]
		dstore := dses[i]

		qi := childQuery
		qi.Prefix = rests[i]
		results, err := dstore.Query(qi)

		if err != nil {
			_ = queries.close()
			return nil, err
		}
		queries.addResults(mount, results)
	}

	qr := query.ResultsFromIterator(master, query.Iterator{
		Next:  queries.next,
		Close: queries.close,
	})

	if len(master.Filters) > 0 {
		for _, f := range master.Filters {
			qr = query.NaiveFilter(qr, f)
		}
	}

	if master.Offset > 0 {
		qr = query.NaiveOffset(qr, master.Offset)
	}

	if master.Limit > 0 {
		qr = query.NaiveLimit(qr, master.Limit)
	}

	return qr, nil
}

// Close closes all mounted datastores.
func (d *Datastore) Close() error {
	var merr error
	for _, d := range d.mounts {
		err := d.Datastore.Close()
		if err != nil {
			merr = multierr.Append(merr, fmt.Errorf(
				"closing datastore at %s: %w",
				d.Prefix.String(),
				err,
			))
		}
	}
	return merr
}

// DiskUsage returns the sum of DiskUsages for the mounted datastores.
// Non PersistentDatastores will not be accounted.
func (d *Datastore) DiskUsage() (uint64, error) {
	var (
		merr    error
		duTotal uint64 = 0
	)
	for _, d := range d.mounts {
		du, err := ds.DiskUsage(d.Datastore)
		duTotal += du
		if err != nil {
			merr = multierr.Append(merr, fmt.Errorf(
				"getting disk usage at %s: %w",
				d.Prefix.String(),
				err,
			))
		}
	}
	return duTotal, merr
}

type mountBatch struct {
	mounts map[string]ds.Batch
	lk     sync.Mutex

	d *Datastore
}

// Batch returns a batch that operates over all mounted datastores.
func (d *Datastore) Batch() (ds.Batch, error) {
	return &mountBatch{
		mounts: make(map[string]ds.Batch),
		d:      d,
	}, nil
}

func (mt *mountBatch) lookupBatch(k key.Key) (ds.Batch, key.Key, error) {
	mt.lk.Lock()
	defer mt.lk.Unlock()

	child, loc, rest := mt.d.lookup(k)
	t, ok := mt.mounts[loc.String()]
	if !ok {
		bds, ok := child.(ds.Batching)
		if !ok {
			return nil, key.NewStrKey(""), ds.ErrBatchUnsupported
		}
		var err error
		t, err = bds.Batch()
		if err != nil {
			return nil, key.NewStrKey(""), err
		}
		mt.mounts[loc.String()] = t
	}
	return t, rest, nil
}

func (mt *mountBatch) Put(key key.Key, val []byte) error {
	t, rest, err := mt.lookupBatch(key)
	if err != nil {
		return err
	}

	return t.Put(rest, val)
}

func (mt *mountBatch) Delete(key key.Key) error {
	t, rest, err := mt.lookupBatch(key)
	if err != nil {
		return err
	}

	return t.Delete(rest)
}

func (mt *mountBatch) Commit() error {
	mt.lk.Lock()
	defer mt.lk.Unlock()

	var merr error
	for p, t := range mt.mounts {
		if err := t.Commit(); err != nil {
			merr = multierr.Append(merr, fmt.Errorf(
				"committing batch to datastore at %s: %w",
				p, err,
			))
		}
	}
	return merr
}

func (d *Datastore) Check() error {
	var merr error
	for _, m := range d.mounts {
		if c, ok := m.Datastore.(ds.CheckedDatastore); ok {
			if err := c.Check(); err != nil {
				merr = multierr.Append(merr, fmt.Errorf(
					"checking datastore at %s: %w",
					m.Prefix.String(),
					err,
				))
			}
		}
	}
	return merr
}

func (d *Datastore) Scrub() error {
	var merr error
	for _, m := range d.mounts {
		if c, ok := m.Datastore.(ds.ScrubbedDatastore); ok {
			if err := c.Scrub(); err != nil {
				merr = multierr.Append(merr, fmt.Errorf(
					"scrubbing datastore at %s: %w",
					m.Prefix.String(),
					err,
				))
			}
		}
	}
	return merr
}

func (d *Datastore) CollectGarbage() error {
	var merr error
	for _, m := range d.mounts {
		if c, ok := m.Datastore.(ds.GCDatastore); ok {
			if err := c.CollectGarbage(); err != nil {
				merr = multierr.Append(merr, fmt.Errorf(
					"gc on datastore at %s: %w",
					m.Prefix.String(),
					err,
				))
			}
		}
	}
	return merr
}
