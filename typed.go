package kv

import (
	"encoding/json"
	"fmt"
	"iter"
)

// Codec converts between a Go value and its on-disk byte representation.
type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

// JSONCodec is the default Codec, using encoding/json.
type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }

func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

// Bucket is a typed convenience layer over DB's raw []byte API: keys are
// namespaced under prefix (so multiple buckets can share one DB) and
// values are (de)serialized through codec.
type Bucket[T any] struct {
	db     *DB
	prefix string
	codec  Codec[T]
}

// NewBucket returns a Bucket over db, namespacing every key under prefix.
// The prefix is prepended verbatim — no separator is inserted — so end it
// with a delimiter (e.g. "users:"): otherwise bucket "a" with key "bc"
// collides with bucket "ab" with key "c", and iterating bucket "user"
// also yields keys written by a bucket whose prefix merely extends it
// ("users").
func NewBucket[T any](db *DB, prefix string, codec Codec[T]) *Bucket[T] {
	return &Bucket[T]{db: db, prefix: prefix, codec: codec}
}

func (b *Bucket[T]) fullKey(key string) []byte {
	return append([]byte(b.prefix), key...)
}

func (b *Bucket[T]) Put(key string, value T) error {
	data, err := b.codec.Encode(value)
	if err != nil {
		return err
	}
	return b.db.Put(b.fullKey(key), data)
}

func (b *Bucket[T]) Get(key string) (T, error) {
	var zero T
	data, err := b.db.Get(b.fullKey(key))
	if err != nil {
		return zero, err
	}
	return b.codec.Decode(data)
}

func (b *Bucket[T]) Delete(key string) error {
	return b.db.Delete(b.fullKey(key))
}

// Has reports whether key currently has a live value in this bucket.
func (b *Bucket[T]) Has(key string) (bool, error) {
	return b.db.Has(b.fullKey(key))
}

// BucketIterator walks a bucket's entries with the bucket prefix stripped
// from keys and values decoded through the bucket's codec.
type BucketIterator[T any] struct {
	it        *Iterator
	codec     Codec[T]
	prefixLen int
	key       string
	val       T
	err       error
}

// Iterator returns a BucketIterator over this bucket's entries
// (optionally further restricted by a sub-prefix within the bucket).
func (b *Bucket[T]) Iterator(subPrefix string) *BucketIterator[T] {
	return &BucketIterator[T]{
		it:        b.db.Iterator(IterOptions{Prefix: b.fullKey(subPrefix)}),
		codec:     b.codec,
		prefixLen: len(b.prefix),
	}
}

// Next advances to the next entry. It returns false once exhausted or on
// a read/decode error; check Err to distinguish.
func (it *BucketIterator[T]) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.it.Next() {
		it.err = it.it.Err()
		return false
	}
	v, err := it.codec.Decode(it.it.Value())
	if err != nil {
		it.err = fmt.Errorf("kv: decode value for key %q: %w", it.it.Key(), err)
		return false
	}
	it.key = string(it.it.Key()[it.prefixLen:])
	it.val = v
	return true
}

func (it *BucketIterator[T]) Key() string { return it.key }
func (it *BucketIterator[T]) Value() T    { return it.val }

// Err returns the first read or decode error encountered by Next, if any.
// A nil Err after Next returns false means the iterator is exhausted.
func (it *BucketIterator[T]) Err() error { return it.err }

// Close releases the underlying iterator. Safe to call multiple times.
func (it *BucketIterator[T]) Close() error { return it.it.Close() }

// All returns an iterator over the bucket's decoded entries, for use with
// range-over-func. A read or decode error silently ends the iteration
// early; use Iterator directly when errors must be distinguished from
// exhaustion.
func (b *Bucket[T]) All(subPrefix string) iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		it := b.Iterator(subPrefix)
		defer it.Close()
		for it.Next() {
			if !yield(it.key, it.val) {
				return
			}
		}
	}
}
