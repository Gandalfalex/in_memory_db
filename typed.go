package kv

import "encoding/json"

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

// Iterator returns an Iterator over this bucket's keys (optionally
// further restricted by a sub-prefix within the bucket).
func (b *Bucket[T]) Iterator(subPrefix string) *Iterator {
	return b.db.Iterator(IterOptions{Prefix: b.fullKey(subPrefix)})
}
