# kv

```
import kv "github.com/Gandalfalex/in_memory_db"
```

Embedded storage for single-process hosts with very little RAM and a
single CPU core. Not one data structure — a family of three, each shaped
for a different access pattern, sharing one discipline: never hold more
in RAM than the access pattern actually requires.

| Tier | Access pattern | On disk | In RAM |
|---|---|---|---|
| [`DB`](#db) | frequent random Get/Put | binary segment log | full index (pointer-free hash table) |
| [`FileIndex`](#fileindex) | frequent random Get/Put, needs a plain-text file | JSONL, human/tool-editable | full index (`map[string]lineLoc`) |
| [`SortedIndex`](#sortedindex) | rare bursts, sort/filter/range over huge data | sorted directory, external-merge-sort built | sparse sample + Bloom filter only |

All three are read through the same shape — `Get(key) ([]byte, error)`
returning `kv.ErrNotFound`, `Has(key) (bool, error)` — captured in the
[`Reader`](#reader-and-store-interfaces) interface if you want to write
code generic over "whichever store the caller has". `DB` and `FileIndex`
(the two writable tiers) share a further `Store` interface, which is what
lets `Bucket`'s typed convenience layer wrap either one. See `example/`
for a complete runnable tour of all three.

## Package layout

The public API is one package (`import kv "github.com/.../in_memory_db"`)
— but the three tiers' actual implementations live in separate,
unimportable `internal/` packages, each only visible to the other packages
in this module:

- `internal/kvtypes` — the shared vocabulary all three tiers depend on:
  sentinel errors, `KeyFunc`, `Iterator`/`IterOptions`, and the fixed-width
  binary I/O helpers every on-disk format uses. No dependencies of its own.
- `internal/bitcask` — the `DB` engine.
- `internal/fileindex` — the `FileIndex` engine.
- `internal/sortedindex` — the `SortedIndex` engine, including its build
  (external merge sort), incremental refresh, and Bloom filter machinery.
- `internal/pool` — the generic idle-TTL pool behind all three managers.
- `internal/mmapfile` — the mmap wrapper `internal/bitcask` uses.

The root package is a thin façade over these: type aliases
(`type DB = bitcask.DB`, so `kv.DB` *is* `bitcask.DB`, not a wrapper with
its own method set) plus one-line constructor wrappers, `Bucket`/`Codec`
(genuinely cross-tier, since it's written against `Store` rather than any
one engine), `FileIndexStore` (the `FileIndex`-to-`Store` adapter), and
the three pooled managers (`Manager`, `FileIndexManager`,
`SortedIndexManager` — orchestration that ties one engine package to
`internal/pool`, so neither needs to know the other exists).

The point: nothing outside this module can import `internal/bitcask` and
reach into, say, its hash index or segment format directly — even code
elsewhere in this module can't reach across tiers except through the
shared interfaces (`Reader`, `Store`, `KeyFunc`) `internal/kvtypes`
defines. Splitting `bloom.go` from `sortedindex.go`'s internals, or
`Bucket` from `DB`'s, used to be enforced only by convention (same
package, nothing stopped a shortcut); now the compiler enforces it.

## DB

Bitcask-style log-structured hash table for a moderate dataset under
frequent random access.

```go
db, err := kv.Open(kv.DefaultOptions("/var/lib/myapp/data"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()

db.Put([]byte("user:1"), []byte("alice"))
v, err := db.Get([]byte("user:1"))

// range-over-func iteration (read errors end the loop silently):
for k, v := range db.All(kv.IterOptions{Prefix: []byte("user:")}) {
    fmt.Println(string(k), string(v))
}

// explicit Iterator when errors must be distinguished from exhaustion:
it := db.Iterator(kv.IterOptions{Prefix: []byte("user:"), Sorted: true})
defer it.Close()
for it.Next() {
    fmt.Println(string(it.Key()), string(it.Value()))
}
if err := it.Err(); err != nil {
    log.Fatal(err)
}

db.Delete([]byte("user:1"))
```

Lookups return `kv.ErrNotFound` for absent keys; invalid keys return
`kv.ErrEmptyKey` or `kv.ErrKeyTooLarge` (keys are capped at
`kv.MaxKeyLen` = 65535 bytes). All are `errors.Is`-comparable.

For typed values, wrap the raw `[]byte` API in a `Bucket`. End the bucket
prefix with a delimiter (it is prepended verbatim, so `"user"` would also
match a `"users:"` bucket's keys):

```go
type User struct{ Name string; Age int }

users := kv.NewBucket[User](db, "users:", kv.JSONCodec[User]{})
users.Put("alice", User{Name: "Alice", Age: 30})
u, err := users.Get("alice")

// typed iteration: keys come back prefix-stripped, values decoded
for name, u := range users.All("") {
    fmt.Println(name, u.Age)
}
```

`Codec[T]` is an interface (`Encode(T) ([]byte, error)`,
`Decode([]byte) (T, error)`) — `JSONCodec[T]` is the built-in
implementation; swap in your own for a different wire format (protobuf,
gob, msgpack) without changing `Bucket`.

`Bucket` is written against `Store`, not `*DB` directly, so it works over
`FileIndex` too — see [Bucket over FileIndex](#bucket-over-fileindex) in
the `FileIndex` section below.

Maintenance and introspection:

```go
db.Sync()    // fsync the active segment (when SyncOnWrite is off)
db.Compact() // run compaction now instead of waiting for the 30s ticker
s := db.Stats() // Keys, Segments, DeadBytes, LastCompactionErr
```

For many independently-sized `DB`s that shouldn't all pay RAM rent at
once, use `Manager` — see [Managers](#managers).

### DB design

- **On disk**: append-only, 64MB segment files. Each record is
  `CRC32 | timestamp | keyLen | valLen | flags | key | value` (21 bytes of
  header overhead). Updates and deletes never rewrite in place — a delete
  appends a tombstone record.
- **In RAM**: a custom open-addressing hash index (`index.go`) mapping key
  → (segment, offset, length), backed by a chunked byte arena for key
  storage — deliberately not `map[string]indexEntry`. At a few million
  keys, a custom structure only saves ~5-9% of memory over a plain map
  (key bytes dominate either way); the real reason to avoid `map[string]T`
  here is garbage-collector pressure. A Go map with millions of
  string-keyed entries makes the GC visit millions of pointers on every
  mark phase. With one CPU core and no spare capacity to absorb that scan
  cost, a pointer-free, arena-backed table is a large, measurable win.
- **Concurrency**: a single `sync.RWMutex` guards the index; one active
  segment is written to at a time. No sharding or lock-striping — on one
  physical CPU there is no real contention to relieve, and sharding would
  only spend extra memory (N index headers instead of one) for nothing.
- **Recovery**: an index snapshot/checkpoint is written on a clean
  `Close()`. The next `Open()` loads it and replays only the bytes
  appended to the active segment since, instead of rescanning every
  segment. Any background compaction pass invalidates the snapshot (since
  it changes segment layout), so a crash shortly after heavy compaction
  falls back to a full scan — a deliberate, safe tradeoff. A checksum
  failure or torn write anywhere in a segment truncates recovery of that
  segment at that point; `Open()` never fails outright because of it.
- **Compaction**: a background goroutine tracks dead bytes per segment
  (O(1) bookkeeping on every overwrite/delete) and, once a segment crosses
  the configured dead-byte ratio (default 50%), streams its still-live
  records into a shared compaction-output segment and drops the old file.
  Each record's liveness check and relocation happens in one locked
  critical section, so there's no window where the index can point at a
  half-moved value.
- **Reads: mmap when hot, pread when not** (`segment.go`, `mincore_unix.go`).
  Touching an mmap'd page that isn't resident causes a major page fault —
  the OS thread blocks until the kernel loads it from disk, and critically,
  the Go runtime scheduler has no visibility into this (it's not a
  syscall, just a memory load that happened to block). On a host with a
  single CPU, that can stall the entire process: there's no second P for
  other goroutines to run on while the one P sits behind an invisible
  fault. `Get` guards against this with `mincore(2)`: read straight from
  the mmap only once a range is confirmed resident; otherwise fall back to
  a real positioned read (`pread`), which the runtime *does* recognize as
  blocking and can schedule around normally. Each segment caches confirmed
  hits in a small per-page bitmap (cleared every 60s) so this costs a
  syscall once per cold page, not on every read of already-hot data —
  measured overhead on a fully hot working set is within noise (~76ns/op
  vs. ~80ns/op uncached). This whole design — including the mmap-length
  page-rounding in `internal/mmapfile` to avoid a documented SIGBUS
  failure mode in `copy()`'s internal wide reads near a mapping's end — is
  adapted from [Phuong Le's "mmap vs pread in a real Go storage
  engine"](https://internals-for-interns.com/posts/mmap-vs-pread-go-storage-engine/),
  describing the same tradeoff in VictoriaMetrics/VictoriaLogs.

#### Measured memory/CPU characteristics

`scale_test.go` (`go test -tags scale -run TestMemoryBoundAtScale`)
inserts 4,000,000 records (20-byte keys, 100-byte values) on a single
core:

```
final: n=4000000 HeapAlloc=336MB HeapSys=707MB NumGC=11 PauseTotal=0ms
```

~336MB heap for 4M records, and effectively no GC pause time — the
pointer-free index is, as intended, close to invisible to the collector.

## FileIndex

Same "frequent random Get/Put" shape as `DB`, different constraint: the
backing file must stay plain, human/tool-readable JSONL — no binary
envelope, no segments, nothing kv-owned beyond the bytes you write.

```go
fi, err := kv.OpenFileIndex("/var/lib/myapp/events.jsonl", kv.JSONStringKey("id"))
if err != nil {
    log.Fatal(err)
}
defer fi.Close()

fi.Put([]byte(`{"id":"evt-1","type":"signup"}`)) // key derived from the line via KeyFunc
line, err := fi.Get([]byte("evt-1"))
```

`KeyFunc` is the plug point: `JSONStringKey(field)` extracts a top-level
JSON string field without unmarshalling the whole line; supply your own
`func(line []byte) (key []byte, ok bool)` for a different line format
(CSV, a different serialization) or key derivation.

The index (`key -> line offset`) is rebuilt in RAM by one sequential scan
on `Open`, last line wins per key. `Delete` is index-only — it does not
survive reopen, since the file is append-only and carries no tombstone;
write a line your `KeyFunc` maps to a deletion marker if you need durable
removal. See `fileindex.go`'s doc comments for the full contract.

### Bucket over FileIndex

`FileIndexStore` adapts a `*FileIndex` to `Store` so `Bucket` works over
it exactly like it works over `*DB` — same `Put`/`Get`/`Delete`/`Has`/`All`
calls, only the constructor differs:

```go
fi, err := kv.OpenFileIndex("/var/lib/myapp/users.jsonl", kv.JSONStringKey("id"))
store := kv.NewFileIndexStore(fi, kv.JSONLineCodec{}) // KeyField defaults to "id"

users := kv.NewBucket[User](store, "", kv.JSONCodec[User]{})
users.Put("alice", User{Name: "Alice", Age: 30})
u, err := users.Get("alice")
```

`JSONLineCodec` writes `{"id":"<key>","value":<value>}` — `Build` is the
write-side counterpart to `KeyFunc`'s read-side extraction, and the two
must agree on the field name (`JSONLineCodec{KeyField: "uuid"}` pairs with
`JSONStringKey("uuid")`) or a written entry can never be read back by key.
`LineCodec` is itself an interface, so a non-JSON line format needs its
own `Build`/`Value` implementation, same as a non-JSON `KeyFunc` would.

## SortedIndex

Built for the opposite access pattern: a dataset far too large to
RAM-index (tens of millions of lines and up), queried in rare bursts
(needs sort, filter, range), otherwise idle.

```go
// Build once (or whenever the sources change — see EnsureFresh below):
// sourcePaths is precedence order, lowest first — a one-shot base file
// followed by incremental change files, later file wins on a key conflict.
err := kv.BuildSortedIndex(
    ctx, // checked periodically; cancel to abort a long build early
    []string{"report-files.jsonl", "report-changes.jsonl"},
    kv.JSONStringKey("id"),
    "cache/report.sidx",
    kv.SortedIndexOptions{}, // zero value = library defaults
)

si, err := kv.OpenSortedIndex("cache/report.sidx", kv.JSONStringKey("id"))
defer si.Close()

line, err := si.Get([]byte("r1"))         // ErrNotFound if absent
for k, line := range si.All() { ... }     // full scan, ascending key order
for k, line := range si.Prefix([]byte("r")) { ... } // range scan

// Generic predicate combinator over the raw line bytes:
for k, line := range kv.FilterAll(si.All(), func(line []byte) bool {
    return bytes.Contains(line, []byte(`"status":"pending"`))
}) { ... }
```

`EnsureFresh` is the usual entry point instead of calling `BuildSortedIndex`
directly: it stats the sources, and only rebuilds if they're missing or
have actually changed (size/mtime) since the cache was built — a cheap
stat on the common path, the full rebuild cost only when the data moved.

```go
si, err := kv.EnsureFresh(ctx, sourcePaths, sidxPath, kv.JSONStringKey("id"), kv.SortedIndexOptions{})
```

Concurrent `BuildSortedIndex`/`EnsureFresh` calls for the same `sidxPath` are
serialized within one process (a per-path lock, not a file lock — see
`sortedindex_build.go`'s `buildLocks`), so two goroutines racing to
rebuild the same cache can't corrupt it or leave a stale result on top of
a fresh one. This does not extend across processes: two separate
processes (or two `SortedIndexManager`s) pointed at the same `CacheDir`
are not coordinated, consistent with the rest of the package being
single-process by design (see [Scope](#scope)). `SortedIndexManager.Acquire`
always passes `context.Background()` internally — pool.go's shared
`Acquire`/`openRes` machinery has no `ctx` parameter (`Manager`/
`FileIndexManager` don't need one) — so a build triggered via `Acquire`
can't be cancelled; call `BuildSortedIndex`/`EnsureFresh` directly first
(e.g. in a deploy step) if you need that.

### SortedIndex design

- **Build**: external merge sort (`sortedindex_build.go`) — sources are
  scanned in RAM-bounded chunks (`ChunkEntries`, default 2M entries),
  each chunk sorted and spilled to a run file, then every run is k-way
  merged into the final sorted directory. RAM at any point is bounded by
  `ChunkEntries`, never by the combined source size, so this scales past
  available RAM by construction.
- **Read**: a small in-RAM sparse directory (every `SparseInterval`-th
  key, default 4096) binary-searches to a starting offset, then one
  bounded sequential scan of the on-disk sorted directory (at most
  `SparseInterval` entries) finds the exact entry, then one `pread` of
  the owning source file gets the line. RAM cost is the sparse sample —
  O(1) in dataset size, not O(n).
- **Bloom filter**: an optional sidecar (`bloom.go`, `BloomFPR`, default
  1%) answers "definitely absent" for a miss with zero disk I/O,
  short-circuiting before the sparse-scan path. Costs ~9.6 bits/key
  flat, regardless of key length — cheaper than the sparse directory
  itself when keys are long. Set `BloomFPR` negative to skip it.
- **Multi-source merge**: duplicate keys resolve by file precedence
  (later `sourcePaths` entry wins), then by later byte offset within a
  file — the direct multi-file generalization of `FileIndex`'s
  "last line wins".
- **Freshness** (`sortedindex_sources.go`): a `.sources` sidecar records
  each source's path/size/mtime at build time; `EnsureFresh` compares a
  fresh `stat` against it to decide rebuild-or-reopen without touching
  file contents.
- **Incremental refresh** (`sortedindex_refresh.go`): when the only
  difference from the recorded state is one or more *new* sources
  appended after the existing ones — nothing removed, reordered, or
  itself modified — `EnsureFresh` folds just the new data in rather than
  rebuilding from scratch: the existing sidx's entries are reused
  wholesale (read once, sequentially, not re-parsed) as one more input to
  the same merge `BuildSortedIndex` uses, alongside a normal scan+sort of
  only the new sources. Cost is proportional to the new data plus one
  sequential pass over the existing index, not to the whole dataset —
  this is what keeps a daily-changing delta file cheap against a 78M-row
  base that never moves. Any other kind of change (an existing source's
  content, size, or position changed) falls back to a full rebuild —
  deliberately: telling "safely appended" apart from "possibly modified"
  is only reliable when framed as "is this whole file new," not by
  guessing from a stat alone whether an existing file's bytes are still a
  prefix of themselves.
- **Corruption checking**: every sidecar (`.sidx`, `.sparse`, `.bloom`,
  `.sources`) carries a trailing CRC32, verified on every `Open` before
  any of it is trusted — a bad magic byte, truncation, or a checksum
  mismatch fails `Open` outright rather than risking a silently wrong
  read later. For `.sidx` this is a full sequential pass over the whole
  entries region (the same tradeoff `DB`'s own `index.snapshot` already
  makes for its checkpoint file), paid once per `Open`/reopen, not per
  `Get`.

## Managers

Three pooled wrappers — `Manager` (for `DB`), `FileIndexManager`,
`SortedIndexManager` — share one generic implementation
(`pool[T closer]` in `pool.go`): lazy open on first `Acquire`, refcounted
while handles are outstanding, closed by a background reaper after
`IdleTTL` with no outstanding handles. The point is the same in all
three: a rarely-used named dataset costs nothing in RAM between bursts of
use, and the next `Acquire` transparently reopens it.

```go
m, err := kv.NewFileIndexManager(kv.FileIndexManagerOptions{
    BaseDir: "/var/lib/myapp/events",
    KeyFunc: kv.JSONStringKey("id"),
    IdleTTL: 5 * time.Minute,
})
defer m.Close()

h, err := m.Acquire("2026-07-22.jsonl") // opens BaseDir/2026-07-22.jsonl
defer h.Close()                          // releases the handle; the manager owns the resource
h.Put(...)
```

`SortedIndexManager` differs in one way: since a `SortedIndex` is built
from a caller-tracked set of files (not one fixed path per name), its
`Acquire(name, sourcePaths)` takes the ordered source list on every call
— the manager only owns where the built cache lives, not which files
make up a dataset.

```go
sm, err := kv.NewSortedIndexManager(kv.SortedIndexManagerOptions{
    CacheDir: "/var/lib/myapp/cache",
    KeyFunc:  kv.JSONStringKey("id"),
    IdleTTL:  5 * time.Minute,
})
defer sm.Close()

h, err := sm.Acquire("report", []string{"report-files.jsonl", "report-changes.jsonl"})
defer h.Close()
```

## Reader and Store interfaces

```go
type Reader interface {
    Get(key []byte) ([]byte, error)
    Has(key []byte) (bool, error)
}

type Store interface {
    Reader
    Put(key, value []byte) error
    Delete(key []byte) error
    Iterator(opts IterOptions) *Iterator
    DecodeValue(raw []byte) (value []byte, ok bool)
}
```

`*DB`, `*FileIndex`, and `*SortedIndex` all satisfy `Reader` as-is — it's
the plug point for code written against "whichever store the caller has"
(a read-through cache, a metrics-wrapping decorator, a test fake) instead
of a concrete type.

`*DB` also satisfies `Store` directly. `*FileIndex` doesn't:
`FileIndex.Put(line []byte)` writes one self-describing line and derives
its key via `KeyFunc`, a genuinely different write model from `DB`'s
explicit `Put(key, value []byte)` — not a signature accident, since
`FileIndex`'s whole point is staying plain-text/tool-readable, which a raw
key+value pair can't express on its own. `FileIndexStore` bridges the two
via a `LineCodec` instead of forcing them into one shape — see
[Bucket over FileIndex](#bucket-over-fileindex). `SortedIndex` has no
`Put` at all and never satisfies `Store`; it only ever satisfies `Reader`
(read-only by design).

## Scope

This is an embedded, single-process store, not a distributed or networked
one. `DB` writes are atomic per key, not across multiple keys — there is
no multi-key transaction support. `DB.All`/`Iterator` prefix iteration
snapshots the set of matching *keys* at creation time (not their values),
so its memory cost scales with the number of matched keys; the index is a
hash table, not a sorted structure, so default iteration order has no
lexicographic meaning — `IterOptions.Sorted` sorts the already-materialized
key snapshot at iterator creation (O(n log n), no extra memory).
`SortedIndex`, by contrast, is sorted on disk by construction, so its
iteration order costs nothing extra. Keys are capped at `MaxKeyLen`
(65535 bytes) throughout.

The mmap-backed implementation targets unix platforms (darwin, linux,
etc.) via `syscall.Mmap`. A pure-file-I/O fallback exists for other
platforms (`internal/mmapfile/mmapfile_other.go`) so the package still
builds and functions correctly there, but it loses the zero-copy /
OS-page-cache behavior the design relies on and is not the primary target.
`SortedIndex` uses plain `pread`/`pwrite` throughout (not mmap), so it has
no such platform split.

## License

MIT — see [LICENSE](LICENSE).
