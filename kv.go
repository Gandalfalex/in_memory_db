package kv

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

const maxKeyLen = math.MaxUint16 // indexSlot.keyLen is a uint16

// Put inserts or overwrites key's value.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("kv: key must not be empty")
	}
	if len(key) > maxKeyLen {
		return fmt.Errorf("kv: key of %d bytes exceeds max length %d", len(key), maxKeyLen)
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
		return fmt.Errorf("kv: key must not be empty")
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
	// Reverse iterates matched keys in reverse of their (arbitrary,
	// hash-order) snapshot order. Since the index is a hash table, not a
	// sorted structure, "reverse" only means "the other direction through
	// an unordered snapshot" — there is no lexicographic ordering
	// guarantee either direction.
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

	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	return &Iterator{db: db, keys: keys, pos: -1}
}

// Next advances to the next matching key, skipping any that were deleted
// or already garbage-collected since the iterator was created. It returns
// false once exhausted.
func (it *Iterator) Next() bool {
	if it.closed {
		return false
	}
	for it.pos++; it.pos < len(it.keys); it.pos++ {
		key := it.keys[it.pos]
		it.db.mu.RLock()
		val, err := it.db.getLocked(key)
		it.db.mu.RUnlock()
		if err == nil {
			it.curKey, it.curVal = key, val
			return true
		}
	}
	return false
}

func (it *Iterator) Key() []byte   { return it.curKey }
func (it *Iterator) Value() []byte { return it.curVal }

// Close releases the iterator's snapshot. Safe to call multiple times.
func (it *Iterator) Close() error {
	it.closed = true
	it.keys = nil
	it.curKey = nil
	it.curVal = nil
	return nil
}
