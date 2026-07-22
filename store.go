package kv

// Reader is the read-only contract shared by every lookup-capable store
// in this package: *DB, *FileIndex, and *SortedIndex all satisfy it
// as-is, with no adapter needed. It exists as a plug point for code that
// wants to work against "whichever store the caller has" — a read-through
// cache, a metrics-wrapping decorator, a test fake — without depending on
// a concrete type. FilterAll's seq argument is the same idea one level up
// (works over any iterator, not tied to SortedIndex).
type Reader interface {
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

// Store is Reader plus the mutating and iterating operations Bucket needs.
// *DB satisfies it directly — its Put/Delete/Iterator already have this
// exact shape. *FileIndex does not: FileIndex.Put(line) writes one
// self-describing line and derives its key via KeyFunc, a genuinely
// different write model from DB's explicit Put(key, value) — not a
// signature accident to paper over, since FileIndex's whole point is
// staying plain-text/tool-readable, which an envelope-free key+value pair
// can't express on its own. FileIndexStore bridges that gap with an
// explicit LineCodec instead of forcing the two shapes together, so
// Bucket (and anything else written against Store) can wrap either
// backend. SortedIndex has no Put at all and so never satisfies Store —
// it is read-only by design (see its doc comment); it only ever
// satisfies Reader.
type Store interface {
	Reader
	Put(key, value []byte) error
	Delete(key []byte) error
	Iterator(opts IterOptions) *Iterator
	// DecodeValue extracts Codec-ready value bytes from raw bytes returned
	// by Iterator's Value() (or Get, though Get already applies this
	// internally for FileIndexStore) — identity for DB, whose stored bytes
	// already are the value; LineCodec.Value for FileIndexStore, whose
	// Iterator yields whole lines, not bare values.
	DecodeValue(raw []byte) (value []byte, ok bool)
}

// DecodeValue satisfies Store: DB's Get/Iterator already yield bare
// values with no envelope, so this is the identity.
func (db *DB) DecodeValue(raw []byte) ([]byte, bool) { return raw, true }

var (
	_ Reader = (*DB)(nil)
	_ Reader = (*FileIndex)(nil)
	_ Reader = (*SortedIndex)(nil)

	_ Store = (*DB)(nil)
	_ Store = (*FileIndexStore)(nil)
)
