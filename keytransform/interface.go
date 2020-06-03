package keytransform

import key "github.com/bdware/go-datastore/key"

// KeyMapping is a function that maps one key to annother
type KeyMapping func(key.Key) key.Key

// KeyTransform is an object with a pair of functions for (invertibly)
// transforming keys
type KeyTransform interface {
	ConvertKey(key.Key) key.Key
	InvertKey(key.Key) key.Key
}
