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
//
// To keep many rarely-touched datasets on disk without paying their RAM
// cost permanently, use a Manager: it pools named DBs under one base
// directory, opens each lazily on Acquire, and closes idle ones after
// ManagerOptions.IdleTTL, freeing their index and mmap regions until the
// next Acquire reopens them:
//
//	m, err := kv.NewManager(kv.ManagerOptions{
//		BaseDir: "/var/lib/myapp/stores",
//		IdleTTL: 5 * time.Minute,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer m.Close()
//
//	h, err := m.Acquire("migration-2026-07") // opens BaseDir/migration-2026-07
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer h.Close() // releases the handle; the Manager owns the DB
//	h.Put(...)
package kv
