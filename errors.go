package kv

import (
	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
	"github.com/Gandalfalex/in_memory_db/internal/pool"
)

// These are the same sentinel error values internal/kvtypes defines (and
// internal/bitcask, internal/fileindex, internal/sortedindex all return) —
// re-exported here, not copied, so errors.Is comparisons work identically
// whether a caller checks against kv.ErrNotFound or (from inside this
// module) kvtypes.ErrNotFound.
var (
	// ErrNotFound is returned by Get/Has-style lookups when a key has no
	// live value.
	ErrNotFound = kvtypes.ErrNotFound
	// ErrClosed is returned by any operation on a store after Close has
	// been called.
	ErrClosed = kvtypes.ErrClosed
	// ErrCorrupt is returned when recovery encounters data it cannot trust
	// (e.g. a malformed index snapshot) and no safe fallback applies.
	ErrCorrupt = kvtypes.ErrCorrupt
	// ErrEmptyKey is returned by Put/Get/Has/Delete when key is empty.
	ErrEmptyKey = kvtypes.ErrEmptyKey
	// ErrKeyTooLarge is returned (wrapped, with the offending size) by
	// Put/Delete when key exceeds MaxKeyLen. Test with errors.Is.
	ErrKeyTooLarge = kvtypes.ErrKeyTooLarge
	// ErrNoLineKey is returned by FileIndex.Put when the KeyFunc extracts
	// no key from the line — such a line could never be recovered into the
	// index on reopen, so refusing it keeps the file and index consistent.
	ErrNoLineKey = kvtypes.ErrNoLineKey
	// ErrInvalidLine is returned by FileIndex.Put when the line contains a
	// newline byte (a record is exactly one line).
	ErrInvalidLine = kvtypes.ErrInvalidLine
	// ErrManagerClosed is returned by Acquire after a manager's Close has
	// been called, and by a second call to Close. Shared by Manager,
	// FileIndexManager, and SortedIndexManager.
	ErrManagerClosed = pool.ErrClosed
)
