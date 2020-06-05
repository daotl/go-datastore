// Copyright (c) 2020 The BDWare Authors. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package key

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
)

var ErrNotBytesKey = errors.New("argument is not of type BytesKey")

// BytesKey is a Key implementation backed by byte slice.
type BytesKey struct {
	bytes []byte
}

// NewBytesKey constructs a key from byte slice. it will clean the value.
func NewBytesKey(b []byte) Key {
	k := BytesKey{b}
	return k
}

// KeyWithNamespaces constructs a key out of a namespace slice.
func BytesKeyWithNamespaces(ns [][]byte) Key {
	return BytesKey{bytes.Join(ns, nil)}
}

// Strings is the string value of Key
func (k BytesKey) String() string {
	return string(k.bytes)
}

// KeyType returns the key type (KeyTypeBytes)
func (k BytesKey) KeyType() KeyType {
	return KeyTypeBytes
}

// Bytes returns the string value of Key as a []byte
func (k BytesKey) Bytes() []byte {
	return k.bytes
}

// Equal checks equality of two keys
func (k BytesKey) Equal(k2 Key) bool {
	if k2 == nil {
		return false
	}
	bk2, ok := k2.(BytesKey)
	return ok && bytes.Equal(k.bytes, bk2.bytes)
}

// Less checks whether this key is sorted lower than another.
// Panic if `k2` is not a BytesKey.
func (k BytesKey) Less(k2 Key) bool {
	if k2 == nil {
		return false
	}
	if k2.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.Compare(k.bytes, k2.(BytesKey).bytes) == -1
}

// List returns the `list` representation of this Key.
// Not applicable for BytesKey.
func (k BytesKey) List() []string {
	panic(ErrUnimplemented)
}

// Reverse returns the reverse of this Key.
// Not applicable for BytesKey.
func (k BytesKey) Reverse() Key {
	panic(ErrUnimplemented)
}

// Namespaces returns the `namespaces` making up this Key.
// Not applicable for BytesKey.
func (k BytesKey) Namespaces() []string {
	panic(ErrUnimplemented)
}

// BaseNamespace returns the "base" namespace of this key (path.Base(filename))
// Not applicable for BytesKey.
func (k BytesKey) BaseNamespace() string {
	panic(ErrUnimplemented)
}

// Type returns the "type" of this key (value of last namespace).
// Not applicable for BytesKey.
func (k BytesKey) Type() string {
	panic(ErrUnimplemented)
}

// Name returns the "name" of this key (field of last namespace).
// Not applicable for BytesKey.
func (k BytesKey) Name() string {
	panic(ErrUnimplemented)
}

// Instance returns an "instance" of this type key (appends value to namespace).
// Not applicable for BytesKey.
func (k BytesKey) Instance(s string) Key {
	panic(ErrUnimplemented)
}

// Path returns the "path" of this key (parent + type).
// Not applicable for BytesKey.
func (k BytesKey) Path() Key {
	panic(ErrUnimplemented)
}

// Parent returns the `parent` Key of this Key.
// Not applicable for BytesKey.
func (k BytesKey) Parent() Key {
	panic(ErrUnimplemented)
}

// Child returns the `child` Key of this Key.
//   NewBytesKey({{BYTES1}}).Child(NewBytesKey({{BYTES2}}))
//   NewBytesKey({{BYTES1 || BYTES2}})
// Panic if `k2` is not a BytesKey.
func (k BytesKey) Child(k2 Key) Key {
	if k2 == nil {
		return k
	}
	if k2.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return k.ChildBytes(k2.(BytesKey).bytes)
}

// ChildString returns the `child` Key of this Key -- string helper.
// Not applicable for BytesKey.
func (k BytesKey) ChildString(s string) Key {
	panic(ErrUnimplemented)
}

// ChildBytes returns the `child` Key of this Key -- bytes helper.
//   NewBytesKey({{BYTES1}}).Child({{BYTES2}}))
//   NewBytesKey({{BYTES1 || BYTES2}})
func (k BytesKey) ChildBytes(b []byte) Key {
	kb := make([]byte, len(k.bytes)+len(b))
	copy(kb, k.bytes)
	copy(kb[len(k.bytes):], b)
	return BytesKey{kb}
}

// IsAncestorOf returns whether this key is a prefix of `other` (excluding equals).
//   NewBytesKey({{BYTES1}}).IsAncestorOf(NewBytesKey({{BYTES1 || BYTES2}}))
//   true
// Panic if `other` is not a BytesKey.
func (k BytesKey) IsAncestorOf(other Key) bool {
	if other == nil {
		return false
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	bother := other.(BytesKey)
	return len(bother.bytes) > len(k.bytes) && bytes.HasPrefix(bother.bytes, k.bytes)
}

// IsDescendantOf returns whether this key contains another as a prefix (excluding equals).
//   NewBytesKey({{BYTES1 || BYTES2}}).IsDescendantOf({{BYTES1}})
//   true
// Panic if `other` is not a BytesKey.
func (k BytesKey) IsDescendantOf(other Key) bool {
	if other == nil {
		return true
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return other.(BytesKey).IsAncestorOf(k)
}

// IsTopLevel returns whether this key has only one namespace.
// Not applicable for BytesKey.
func (k BytesKey) IsTopLevel() bool {
	panic(ErrUnimplemented)
}

// HasPrefix returns whether this key contains another as a prefix (including equals).
// Panic if `other` is not a BytesKey.
func (k BytesKey) HasPrefix(other Key) bool {
	if other == nil {
		return true
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.HasPrefix(k.bytes, other.(BytesKey).bytes)
}

// HasPrefix returns whether this key contains another as a suffix (including equals).
// Panic if `other` is not a BytesKey.
func (k BytesKey) HasSuffix(other Key) bool {
	if other == nil {
		return true
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.HasSuffix(k.bytes, other.(BytesKey).bytes)
}

// MarshalJSON implements the json.Marshaler interface,
// keys are represented as base64-encoded JSON string
func (k BytesKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.bytes)
}

// UnmarshalJSON implements the json.Unmarshaler interface,
// keys will decode base64-encoded JSON string to raw bytes
func (k *BytesKey) UnmarshalJSON(data []byte) (err error) {
	k.bytes = []byte{}
	err = json.Unmarshal(data, &k.bytes)
	return
}

// RandomBytesKey returns a randomly (uuid) generated key.
//   RandomBytesKey()
//   NewBytesKey([]byte("f98719ea086343f7b71f32ea9d9d521d"))
func RandomBytesKey() Key {
	return BytesKey{[]byte(strings.Replace(uuid.New().String(), "-", "", -1))}
}
