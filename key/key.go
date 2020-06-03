// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package key

import (
	"errors"
	"fmt"

	dsq "github.com/bdware/go-datastore/query"
)

var ErrUnimplemented = errors.New("function not implemented")

/*
A Key represents the unique identifier of an object.
Keys are meant to be unique across a system.

String-backed Key scheme is inspired by file systems and Google App Engine key model.
String-backed StrKeys are hierarchical, incorporating more and more specific
namespaces. Thus keys can be deemed 'children' or 'ancestors' of other keys::

    StrKey("/Comedy")
    StrKey("/Comedy/MontyPython")

Also, every namespace can be parametrized to embed relevant object
information. For example, the StrKey `name` (most specific namespace) could
include the object type::

    StrKey("/Comedy/MontyPython/Actor:JohnCleese")
    StrKey("/Comedy/MontyPython/Sketch:CheeseShop")
    StrKey("/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender")

*/
type Key interface {
	fmt.Stringer
	// Bytes returns the string value of Key as a []byte
	Bytes() []byte
	// Equal checks equality of two keys
	Equal(k2 Key) bool
	// Less checks whether this key is sorted lower than another.
	Less(k2 Key) bool
	// List returns the `list` representation of this Key.
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").List()
	//   ["Comedy", "MontyPythong", "Actor:JohnCleese"]
	List() []string
	// Reverse returns the reverse of this Key.
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Reverse()
	//   NewStrKey("/Actor:JohnCleese/MontyPython/Comedy")
	Reverse() Key
	// Namespaces returns the `namespaces` making up this Key.
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Namespaces()
	//   ["Comedy", "MontyPython", "Actor:JohnCleese"]
	Namespaces() []string
	// BaseNamespace returns the "base" namespace of this key (path.Base(filename))
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").BaseNamespace()
	//   "Actor:JohnCleese"
	BaseNamespace() string
	// Type returns the "type" of this key (value of last namespace).
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Type()
	//   "Actor"
	Type() string
	// Name returns the "name" of this key (field of last namespace).
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Name()
	//   "JohnCleese"
	Name() string
	// Instance returns an "instance" of this type key (appends value to namespace).
	//   NewStrKey("/Comedy/MontyPython/Actor").Instance("JohnClesse")
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese")
	Instance(s string) Key
	// Path returns the "path" of this key (parent + type).
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Path()
	//   NewStrKey("/Comedy/MontyPython/Actor")
	Path() Key
	// Parent returns the `parent` Key of this Key.
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Parent()
	//   NewStrKey("/Comedy/MontyPython")
	Parent() Key
	// Child returns the `child` Key of this Key.
	Child(k2 Key) Key
	// ChildString returns the `child` Key of this Key -- string helper.
	//   NewStrKey("/Comedy/MontyPython").ChildString("Actor:JohnCleese")
	//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese")
	ChildString(s string) Key
	// ChildBytes returns the `child` Key of this Key -- bytes helper.
	//   NewBytesKey({{BYTES1}}).Child({{BYTES2}}))
	//   NewBytesKey({{BYTES1 || BYTES2}})
	ChildBytes(b []byte) Key
	// IsAncestorOf returns whether this key is a prefix of `other`
	IsAncestorOf(other Key) bool
	// IsDescendantOf returns whether this key contains another as a prefix.
	IsDescendantOf(other Key) bool
	// IsTopLevel returns whether this key has only one namespace.
	IsTopLevel() bool
	// MarshalJSON implements the json.Marshaler interface,
	// keys are represented as JSON strings
	MarshalJSON() ([]byte, error)
}

// KeySlice attaches the methods of sort.Interface to []Key,
// sorting in increasing order.
type KeySlice []Key

func (p KeySlice) Len() int           { return len(p) }
func (p KeySlice) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p KeySlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// EntryKeys
func EntryKeys(e []dsq.Entry) []Key {
	ks := make([]Key, len(e))
	for i, e := range e {
		ks[i] = e.Key
	}
	return ks
}
