package bitcask

import (
	"bytes"
	"fmt"
	"iter"
	"math"
	"time"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

// MaxKeyLen is the largest key size Put accepts, in bytes (the index
// stores key lengths as uint16).
const MaxKeyLen = math.MaxUint16

// Put inserts or overwrites key's value.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return kvtypes.ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return fmt.Errorf("%w: %d bytes > %d", kvtypes.ErrKeyTooLarge, len(key), MaxKeyLen)
	}
	record := encodeRecord(key, value, false, time.Now().UnixNano())

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kvtypes.ErrClosed
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

// Get returns a copy of key's current value, or kvtypes.ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, kvtypes.ErrEmptyKey
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, kvtypes.ErrClosed
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
		return nil, kvtypes.ErrNotFound
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
		return false, kvtypes.ErrEmptyKey
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false, kvtypes.ErrClosed
	}
	_, found := db.idx.get(key)
	return found, nil
}

// DecodeValue satisfies kv.Store at the root: DB's Get/Iterator already
// yield bare values with no envelope, so this is the identity. Defined
// here (not as an alias-attached method at root, which Go doesn't allow
// on a type alias to another package's type) so *DB structurally
// satisfies kv.Store without kv needing any DB-specific adapter.
func (db *DB) DecodeValue(raw []byte) ([]byte, bool) { return raw, true }

// Delete removes key. Deleting an absent key is not an error.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return kvtypes.ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return fmt.Errorf("%w: %d bytes > %d", kvtypes.ErrKeyTooLarge, len(key), MaxKeyLen)
	}
	record := encodeRecord(key, nil, true, time.Now().UnixNano())

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kvtypes.ErrClosed
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

// Iterator returns a new kvtypes.Iterator over keys matching opts.
func (db *DB) Iterator(opts kvtypes.IterOptions) *kvtypes.Iterator {
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

	return kvtypes.NewIterator(keys, opts, func(key []byte) ([]byte, error) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.closed {
			return nil, kvtypes.ErrClosed
		}
		return db.getLocked(key)
	})
}

// All returns an iterator over key/value pairs matching opts, for use
// with range-over-func. A read error silently ends the iteration early;
// use Iterator directly when errors must be distinguished from
// exhaustion.
func (db *DB) All(opts kvtypes.IterOptions) iter.Seq2[[]byte, []byte] {
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
