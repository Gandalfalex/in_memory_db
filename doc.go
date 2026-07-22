// Package kv is embedded storage for single-process hosts with very
// little RAM and a single CPU core: not one data structure but a family
// of three, DB, FileIndex, and SortedIndex, each shaped for a different
// access pattern; see the README for the full design rationale and a
// side-by-side comparison.
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
// via All (range-over-func) or Iterator. For typed values, wrap a Store
// (DB satisfies it directly) in a Bucket, which namespaces keys under a
// prefix and (de)serializes values through a Codec.
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
//
// When the on-disk format must stay a plain, human/tool-readable line
// file (e.g. JSONL) rather than kv's binary segments, use FileIndex: the
// file holds exactly the caller's bytes, one record per line, append-only,
// and the index is pure RAM, rebuilt on Open by one scan with a
// caller-supplied KeyFunc (see JSONStringKey). Put appends and repoints;
// superseded lines stay on disk. FileIndexManager pools named FileIndexes
// with the same Acquire/Release/idle-reap behavior as Manager:
//
//	fi, err := kv.OpenFileIndex("/var/lib/myapp/traces.jsonl", kv.JSONStringKey("id"))
//
// FileIndexStore adapts a FileIndex to Store (via a LineCodec bridging
// its one-line writes onto Store's explicit Put(key, value)), so Bucket
// works over a FileIndex the same way it works over a DB.
//
// When the dataset is too large to index in RAM at all (tens of millions
// of lines and up) but is only queried in rare bursts — sort, filter, or
// range over the whole thing, otherwise idle — use SortedIndex instead:
// BuildSortedIndex writes a sorted directory via external merge sort (RAM
// bounded by SortedIndexOptions.ChunkEntries, never by the source size),
// and EnsureFresh reopens that cache — rebuilding only when the sources
// have actually changed, and folding in a purely-appended new source
// (see sortedindex_refresh.go) without re-scanning the rest — which
// SortedIndexManager pairs with the same idle-TTL reaping as Manager and
// FileIndexManager.
//
// DB, FileIndex, and SortedIndex all satisfy Reader (Get, Has), the plug
// point for code written against "whichever store the caller has". DB
// and FileIndex (via FileIndexStore) further satisfy Store, adding Put,
// Delete, and Iterator — SortedIndex never does; it is read-only by
// design.
package kv
