// Command example is a minimal, runnable tour of the kv package's three
// storage tiers — DB, FileIndex, and SortedIndex — plus their pooled
// managers. Run with `go run ./example`.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	kv "github.com/Gandalfalex/in_memory_db"
)

type user struct {
	Name string
	Age  int
}

func main() {
	demoDB()
	demoFileIndex()
	demoSortedIndex()
}

// demoDB shows the core binary-segment store: raw Put/Get/Delete,
// prefix iteration (unordered and sorted), typed access via Bucket, and
// maintenance calls.
func demoDB() {
	fmt.Println("=== DB ===")
	dir, err := os.MkdirTemp("", "kv-example-db-*")
	must(err)
	defer os.RemoveAll(dir)

	db, err := kv.Open(kv.DefaultOptions(dir))
	must(err)
	defer db.Close()

	must(db.Put([]byte("user:1"), []byte("alice")))
	must(db.Put([]byte("user:2"), []byte("bob")))
	must(db.Put([]byte("order:1"), []byte("widget")))

	v, err := db.Get([]byte("user:1"))
	must(err)
	fmt.Printf("user:1 = %s\n", v)

	fmt.Println("keys with prefix \"user:\" (range-over-func):")
	for k, v := range db.All(kv.IterOptions{Prefix: []byte("user:")}) {
		fmt.Printf("  %s = %s\n", k, v)
	}

	fmt.Println("all keys, sorted, via explicit Iterator:")
	it := db.Iterator(kv.IterOptions{Sorted: true})
	defer it.Close()
	for it.Next() {
		fmt.Printf("  %s = %s\n", it.Key(), it.Value())
	}
	must(it.Err())

	// Typed access: values (de)serialized through a codec, keys namespaced
	// under the bucket prefix and returned prefix-stripped.
	users := kv.NewBucket[user](db, "users:", kv.JSONCodec[user]{})
	must(users.Put("carol", user{Name: "Carol", Age: 35}))
	must(users.Put("dave", user{Name: "Dave", Age: 41}))
	fmt.Println("typed bucket entries:")
	for name, u := range users.All("") {
		fmt.Printf("  %s: %+v\n", name, u)
	}

	must(db.Delete([]byte("user:2")))
	if _, err := db.Get([]byte("user:2")); errors.Is(err, kv.ErrNotFound) {
		fmt.Println("user:2 deleted as expected")
	}

	must(db.Sync())    // make everything written so far durable
	must(db.Compact()) // reclaim dead bytes now rather than on the ticker
	s := db.Stats()
	fmt.Printf("stats: keys=%d segments=%d deadBytes=%d\n\n", s.Keys, s.Segments, s.DeadBytes)
}

// demoFileIndex shows the JSONL-backed sibling of DB: the file stays
// plain, tool-readable JSON lines; the index (key -> line offset) is
// RAM-resident, rebuilt by one scan on open. Good fit for a moderate
// dataset that also needs to be editable/greppable by hand.
func demoFileIndex() {
	fmt.Println("=== FileIndex ===")
	dir, err := os.MkdirTemp("", "kv-example-fileindex-*")
	must(err)
	defer os.RemoveAll(dir)

	fi, err := kv.OpenFileIndex(filepath.Join(dir, "events.jsonl"), kv.JSONStringKey("id"))
	must(err)
	defer fi.Close()

	must(fi.Put([]byte(`{"id":"evt-1","type":"signup"}`)))
	must(fi.Put([]byte(`{"id":"evt-2","type":"login"}`)))

	line, err := fi.Get([]byte("evt-1"))
	must(err)
	fmt.Printf("evt-1 = %s\n", line)

	// FileIndexManager pools named FileIndexes with idle-TTL reaping — the
	// same building block as Manager, for the JSONL side of the library.
	m, err := kv.NewFileIndexManager(kv.FileIndexManagerOptions{
		BaseDir: dir,
		KeyFunc: kv.JSONStringKey("id"),
		IdleTTL: time.Minute,
	})
	must(err)
	defer m.Close()
	h, err := m.Acquire("events.jsonl") // reopens the same file demoFileIndex just wrote
	must(err)
	defer h.Close()
	fmt.Printf("via manager, evt-2 = %s\n", must2(h.Get([]byte("evt-2"))))

	// Bucket works over FileIndex too, via FileIndexStore — same typed
	// Put/Get/All as the DB-backed Bucket above, just a different
	// constructor.
	profiles, err := kv.OpenFileIndex(filepath.Join(dir, "profiles.jsonl"), kv.JSONStringKey("id"))
	must(err)
	defer profiles.Close()
	users := kv.NewBucket[user](kv.NewFileIndexStore(profiles, kv.JSONLineCodec{}), "", kv.JSONCodec[user]{})
	must(users.Put("eve", user{Name: "Eve", Age: 27}))
	fmt.Printf("bucket-over-FileIndex, eve = %+v\n\n", must2(users.Get("eve")))
}

// demoSortedIndex shows the tier built for the opposite access pattern
// from DB/FileIndex: a dataset too large to RAM-index, queried in rare
// bursts (sort/filter/range), otherwise idle. A one-shot base file plus
// an incremental change file are merged, later file wins on a key
// conflict — the same shape as a base manifest plus a changelog.
func demoSortedIndex() {
	fmt.Println("=== SortedIndex ===")
	dir, err := os.MkdirTemp("", "kv-example-sortedindex-*")
	must(err)
	defer os.RemoveAll(dir)

	basePath := filepath.Join(dir, "report-files.jsonl")
	changesPath := filepath.Join(dir, "report-changes.jsonl")
	must(os.WriteFile(basePath, []byte(
		`{"id":"r1","status":"pending"}`+"\n"+
			`{"id":"r2","status":"pending"}`+"\n"+
			`{"id":"r3","status":"pending"}`+"\n",
	), 0o644))
	must(os.WriteFile(changesPath, []byte(
		`{"id":"r1","status":"done"}`+"\n", // overrides the base entry for r1
	), 0o644))

	sidxPath := filepath.Join(dir, "cache", "report.sidx")
	must(os.MkdirAll(filepath.Dir(sidxPath), 0o755))

	// EnsureFresh builds on first call (external merge sort — the only
	// expensive step) and, on any later call, just stats the sources and
	// reopens the cache if nothing changed.
	si, err := kv.EnsureFresh(
		context.Background(), // no build deadline/cancellation needed here
		[]string{basePath, changesPath},
		sidxPath,
		kv.JSONStringKey("id"),
		kv.SortedIndexOptions{}, // zero value = library defaults
	)
	must(err)
	defer si.Close()

	fmt.Printf("r1 = %s (overridden by changes file)\n", must2(si.Get([]byte("r1"))))
	fmt.Println("all entries, sorted:")
	for k, line := range si.All() {
		fmt.Printf("  %s: %s\n", k, line)
	}
	fmt.Println("entries still pending (FilterAll over the sorted scan):")
	for k, line := range kv.FilterAll(si.All(), containsPending) {
		fmt.Printf("  %s: %s\n", k, line)
	}

	// SortedIndexManager pools built indexes with the same idle-TTL
	// reaping as the other two managers; unlike them, the caller passes
	// the ordered source list on every Acquire, since the app (not the
	// library) tracks which files make up a dataset.
	sm, err := kv.NewSortedIndexManager(kv.SortedIndexManagerOptions{
		CacheDir: filepath.Join(dir, "cache2"),
		KeyFunc:  kv.JSONStringKey("id"),
		IdleTTL:  time.Minute,
	})
	must(err)
	defer sm.Close()
	h, err := sm.Acquire("report", []string{basePath, changesPath})
	must(err)
	defer h.Close()
	fmt.Printf("via manager, r2 = %s\n", must2(h.Get([]byte("r2"))))
}

func containsPending(line []byte) bool {
	return bytes.Contains(line, []byte(`"status":"pending"`))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func must2[T any](v T, err error) T {
	must(err)
	return v
}
