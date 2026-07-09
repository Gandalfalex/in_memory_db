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
)
