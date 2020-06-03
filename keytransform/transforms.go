package keytransform

import key "github.com/bdware/go-datastore/key"

// Pair is a convince struct for constructing a key transform.
type Pair struct {
	Convert KeyMapping
	Invert  KeyMapping
}

func (t *Pair) ConvertKey(k key.Key) key.Key {
	return t.Convert(k)
}

func (t *Pair) InvertKey(k key.Key) key.Key {
	return t.Invert(k)
}

var _ KeyTransform = (*Pair)(nil)

// PrefixTransform constructs a KeyTransform with a pair of functions that
// add or remove the given prefix key.
//
// Warning: will panic if prefix not found when it should be there. This is
// to avoid insidious data inconsistency errors.
type PrefixTransform struct {
	Prefix key.Key
}

// ConvertKey adds the prefix.
func (p PrefixTransform) ConvertKey(k key.Key) key.Key {
	return p.Prefix.Child(k)
}

// InvertKey removes the prefix. panics if prefix not found.
func (p PrefixTransform) InvertKey(k key.Key) key.Key {
	if p.Prefix.String() == "/" {
		return k
	}

	if !p.Prefix.IsAncestorOf(k) {
		panic("expected prefix not found")
	}

	s := k.String()[len(p.Prefix.String()):]
	return key.RawStrKey(s)
}

var _ KeyTransform = (*PrefixTransform)(nil)
