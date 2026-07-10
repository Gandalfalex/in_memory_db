package kv

import "errors"

var (
	// ErrNotFound is returned by Get/Has-style lookups when a key has no
	// live value.
	ErrNotFound = errors.New("kv: key not found")
	// ErrClosed is returned by any operation on a DB after Close has been
	// called.
	ErrClosed = errors.New("kv: database is closed")
	// ErrCorrupt is returned when recovery encounters data it cannot trust
	// (e.g. a malformed index snapshot) and no safe fallback applies.
	ErrCorrupt = errors.New("kv: corrupt data")
	// ErrEmptyKey is returned by Put/Get/Has/Delete when key is empty.
	ErrEmptyKey = errors.New("kv: key must not be empty")
	// ErrKeyTooLarge is returned (wrapped, with the offending size) by
	// Put/Delete when key exceeds MaxKeyLen. Test with errors.Is.
	ErrKeyTooLarge = errors.New("kv: key exceeds max length")
	// ErrNoLineKey is returned by FileIndex.Put when the KeyFunc extracts
	// no key from the line — such a line could never be recovered into the
	// index on reopen, so refusing it keeps the file and index consistent.
	ErrNoLineKey = errors.New("kv: keyFunc extracted no key from line")
	// ErrInvalidLine is returned by FileIndex.Put when the line contains a
	// newline byte (a record is exactly one line).
	ErrInvalidLine = errors.New("kv: line must not contain a newline")
)
