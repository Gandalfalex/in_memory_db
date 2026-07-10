// Package kv is an embedded, single-process key-value store: an
// append-only, memory-mapped segment log on disk plus a compact,
// pointer-free in-memory index. It is built for hosts with very little
// RAM and a single CPU core while still tracking millions of small
// records; see the README for the full design rationale.
//
// Open a DB with the recommended defaults:
//
//	db, err := kv.Open(kv.DefaultOptions("/var/lib/myapp/data"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
// The core API is raw bytes: Put, Get, Has, Delete, and prefix iteration
// via All (range-over-func) or Iterator. For typed values, wrap the DB in
// a Bucket, which namespaces keys under a prefix and (de)serializes
// values through a Codec.
//
// Writes are atomic per key; there are no multi-key transactions. With
// Options.SyncOnWrite off (the default) durability is buffered — call
// Sync for an explicit fsync, and Compact to reclaim dead bytes without
// waiting for the background compactor. Lookups of absent keys return
// ErrNotFound; invalid keys return ErrEmptyKey or ErrKeyTooLarge. All
// sentinel errors are errors.Is-comparable.
package kv
