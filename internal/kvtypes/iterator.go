package kvtypes

import (
	"bytes"
	"errors"
	"slices"
)

// IterOptions configures Iterator.
type IterOptions struct {
	// Prefix, if non-empty, restricts iteration to keys with this prefix.
	Prefix []byte
	// Sorted, if true, orders matched keys lexicographically (bytes.Compare)
	// before iterating, instead of the index's arbitrary hash-bucket order.
	// The key list is already fully materialized at Iterator creation time
	// regardless (see Iterator's doc comment below), so this only adds an
	// O(n log n) sort — no additional memory cost.
	Sorted bool
	// Reverse iterates matched keys in the opposite direction: lexicographic
	// descending if Sorted is set, otherwise just "the other way through an
	// arbitrary hash-order snapshot" (no ordering meaning on its own).
	Reverse bool
}

// Iterator walks keys matching IterOptions.Prefix. It snapshots the set of
// matching keys at creation time (not their values), so its memory cost
// scales with the number of matched keys, not the size of their values —
// an unprefixed iterator over millions of keys still holds all of those
// keys in memory at once, by construction of a hash-indexed (not
// range/sorted) store. Values are read from disk lazily, one at a time, as
// Next() advances. The same type serves both DB and FileIndex — it only
// depends on a caller-supplied get callback, not on either engine's
// internals.
type Iterator struct {
	get    func(key []byte) ([]byte, error)
	keys   [][]byte
	pos    int
	curKey []byte
	curVal []byte
	err    error
	closed bool
}

// NewIterator orders keys per opts and wires the lazy value reader. Both
// internal/bitcask and internal/fileindex construct their Iterator this
// way, passing their own locked, closed-aware get function.
func NewIterator(keys [][]byte, opts IterOptions, get func(key []byte) ([]byte, error)) *Iterator {
	if opts.Sorted {
		slices.SortFunc(keys, bytes.Compare)
	}
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	return &Iterator{get: get, keys: keys, pos: -1}
}

// Next advances to the next matching key, skipping any that were deleted
// or already garbage-collected since the iterator was created. It returns
// false once exhausted or on a read error; check Err to distinguish.
func (it *Iterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for it.pos++; it.pos < len(it.keys); it.pos++ {
		key := it.keys[it.pos]
		val, err := it.get(key)
		switch {
		case err == nil:
			it.curKey, it.curVal = key, val
			return true
		case errors.Is(err, ErrNotFound):
			// deleted since the key snapshot was taken; skip
		default:
			it.err = err
			return false
		}
	}
	return false
}

func (it *Iterator) Key() []byte   { return it.curKey }
func (it *Iterator) Value() []byte { return it.curVal }

// Err returns the first read error encountered by Next, if any. A nil Err
// after Next returns false means the iterator is simply exhausted.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's snapshot. Safe to call multiple times.
func (it *Iterator) Close() error {
	it.closed = true
	it.keys = nil
	it.curKey = nil
	it.curVal = nil
	return nil
}
