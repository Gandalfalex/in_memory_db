package kv

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"time"
)

// MaxKeyLen is the largest key size Put accepts, in bytes (the index
// stores key lengths as uint16).
const MaxKeyLen = math.MaxUint16

// Put inserts or overwrites key's value.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return fmt.Errorf("%w: %d bytes > %d", ErrKeyTooLarge, len(key), MaxKeyLen)
	}
	record := encodeRecord(key, value, false, time.Now().UnixNano())

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.ensureCapacity(int64(len(record))); err != nil {
		return err
	}
	off, err := db.active.append(record)
	if err != nil {
		return err
	}
	valOffset := off + headerSize + int64(len(key))
	loc := location{segID: db.active.id, valOffset: uint32(valOffset), valLen: uint32(len(value))}

	prev, hadPrev := db.idx.put(key, loc)
	if hadPrev {
		db.addDeadBytes(prev.segID, recordSize(uint32(len(key)), prev.valLen))
	}

	if db.opts.SyncOnWrite {
		return db.active.sync()
	}
	return nil
}

// Get returns a copy of key's current value, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return db.getLocked(key)
}

// getLocked reads key's value under an already-held lock (RLock or Lock).
// The returned slice is copied out of the segment's mmap'd memory before
// this returns, so it stays valid even if the source segment is later
// compacted away.
func (db *DB) getLocked(key []byte) ([]byte, error) {
	loc, found := db.idx.get(key)
	if !found {
		return nil, ErrNotFound
	}
	seg, ok := db.segments[loc.segID]
	if !ok {
		return nil, fmt.Errorf("kv: internal: segment %d missing for a live key", loc.segID)
	}
	data, err := seg.readAt(int64(loc.valOffset), int64(loc.valLen))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

// Has reports whether key currently has a live value.
func (db *DB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false, ErrClosed
	}
	_, found := db.idx.get(key)
	return found, nil
}

// Delete removes key. Deleting an absent key is not an error.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return fmt.Errorf("%w: %d bytes > %d", ErrKeyTooLarge, len(key), MaxKeyLen)
	}
	record := encodeRecord(key, nil, true, time.Now().UnixNano())

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.ensureCapacity(int64(len(record))); err != nil {
		return err
	}
	if _, err := db.active.append(record); err != nil {
		return err
	}
	// The tombstone record itself carries no live value; once a segment
	// holding it is compacted, the tombstone bytes themselves are dropped.
	db.addDeadBytes(db.active.id, int64(len(record)))

	prev, hadPrev := db.idx.delete(key)
	if hadPrev {
		db.addDeadBytes(prev.segID, recordSize(uint32(len(key)), prev.valLen))
	}

	if db.opts.SyncOnWrite {
		return db.active.sync()
	}
	return nil
}

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
// Next() advances.
type Iterator struct {
	db     *DB
	keys   [][]byte
	pos    int
	curKey []byte
	curVal []byte
	err    error
	closed bool
}

// Iterator returns a new Iterator over keys matching opts.
func (db *DB) Iterator(opts IterOptions) *Iterator {
	db.mu.RLock()
	var keys [][]byte
	db.idx.forEach(func(key []byte, _ location) bool {
		if len(opts.Prefix) > 0 && !bytes.HasPrefix(key, opts.Prefix) {
			return true
		}
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	db.mu.RUnlock()

	if opts.Sorted {
		slices.SortFunc(keys, bytes.Compare)
	}
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	return &Iterator{db: db, keys: keys, pos: -1}
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
		it.db.mu.RLock()
		val, err := it.db.getLocked(key)
		it.db.mu.RUnlock()
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

// All returns an iterator over key/value pairs matching opts, for use
// with range-over-func. A read error silently ends the iteration early;
// use Iterator directly when errors must be distinguished from
// exhaustion.
func (db *DB) All(opts IterOptions) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := db.Iterator(opts)
		defer it.Close()
		for it.Next() {
			if !yield(it.Key(), it.Value()) {
				return
			}
		}
	}
}
