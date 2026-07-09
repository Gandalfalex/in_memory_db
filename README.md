# kv

```
import kv "github.com/Gandalfalex/inmemory_db"
```

An embedded, single-process key-value store built for hosts with very
little RAM and a single CPU core, while still tracking millions of small
records. It trades the ergonomics of "just load everything into a Go map"
for a design that stays memory-safe at that scale: a lean, pointer-free
index in RAM and the actual data memory-mapped on disk.

## Usage

```go
db, err := kv.Open(kv.DefaultOptions("/var/lib/myapp/data"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()

db.Put([]byte("user:1"), []byte("alice"))
v, err := db.Get([]byte("user:1"))

it := db.Iterator(kv.IterOptions{Prefix: []byte("user:")})
defer it.Close()
for it.Next() {
    fmt.Println(string(it.Key()), string(it.Value()))
}

db.Delete([]byte("user:1"))
```

For typed values, wrap the raw `[]byte` API in a `Bucket`:

```go
type User struct{ Name string; Age int }

users := kv.NewBucket[User](db, "users:", kv.JSONCodec[User]{})
users.Put("alice", User{Name: "Alice", Age: 30})
u, err := users.Get("alice")
```

See `example/` for a complete runnable program.

## Design

Bitcask-style log-structured hash table:

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

### Measured memory/CPU characteristics

`scale_test.go` (`go test -tags scale -run TestMemoryBoundAtScale`)
inserts 4,000,000 records (20-byte keys, 100-byte values) on a single
core:

```
final: n=4000000 HeapAlloc=336MB HeapSys=707MB NumGC=11 PauseTotal=0ms
```

~336MB heap for 4M records, and effectively no GC pause time — the
pointer-free index is, as intended, close to invisible to the collector.

## Scope

This is an embedded, single-process store, not a distributed or networked
one. Writes are atomic per key, not across multiple keys — there is no
multi-key transaction support. Prefix iteration snapshots the set of
matching *keys* at creation time (not their values), so its memory cost
scales with the number of matched keys; the index is a hash table, not a
sorted structure, so iteration order (and "reverse") has no lexicographic
meaning. Keys are capped at 65535 bytes.

The mmap-backed implementation targets unix platforms (darwin, linux,
etc.) via `syscall.Mmap`. A pure-file-I/O fallback exists for other
platforms (`internal/mmapfile/mmapfile_other.go`) so the package still
builds and functions correctly there, but it loses the zero-copy /
OS-page-cache behavior the design relies on and is not the primary target.

## License

MIT — see [LICENSE](LICENSE).
