package kv

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeJSONLFile(t *testing.T, path string, lines []string) {
	t.Helper()
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func testSortedIndexManager(t *testing.T, ttl time.Duration) *SortedIndexManager {
	t.Helper()
	m, err := NewSortedIndexManager(SortedIndexManagerOptions{
		CacheDir:     t.TempDir(),
		KeyFunc:      JSONStringKey("id"),
		BuildOptions: SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2},
		IdleTTL:      ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func sortedIndexStatFor(t *testing.T, m *SortedIndexManager, name string) ManagedDBStat {
	t.Helper()
	for _, s := range m.Stats() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no stats entry for %q", name)
	return ManagedDBStat{}
}

func TestSortedIndexManagerAcquireBuildsAndServes(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "report-files.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":1}`, `{"id":"b","v":1}`})

	m := testSortedIndexManager(t, 0)

	h, err := m.Acquire("report", []string{srcPath})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	got, err := h.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":1}` {
		t.Fatalf("Get(a) = %q, %v", got, err)
	}
}

// Concurrent Acquires for the same cold name with different sourcePaths
// race on which list wins the build (documented on Acquire) — this
// doesn't pin which one wins, only that the outcome is sound: both calls
// succeed, both return usable data, and (since the pool caches by name)
// both end up sharing the one resulting *SortedIndex, not two divergent
// ones.
func TestSortedIndexManagerConcurrentAcquireDifferentSourcePaths(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.jsonl")
	pathB := filepath.Join(dir, "b.jsonl")
	writeJSONLFile(t, pathA, []string{`{"id":"x","v":"a"}`})
	writeJSONLFile(t, pathB, []string{`{"id":"x","v":"b"}`})

	m := testSortedIndexManager(t, 0)

	var wg sync.WaitGroup
	handles := make([]*SortedIndexHandle, 2)
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		handles[0], errs[0] = m.Acquire("dataset", []string{pathA})
	}()
	go func() {
		defer wg.Done()
		handles[1], errs[1] = m.Acquire("dataset", []string{pathB})
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Acquire[%d]: %v", i, err)
		}
		defer handles[i].Close()
	}
	got0, err := handles[0].Get([]byte("x"))
	if err != nil {
		t.Fatalf("Get[0]: %v", err)
	}
	if s := string(got0); s != `{"id":"x","v":"a"}` && s != `{"id":"x","v":"b"}` {
		t.Fatalf("Get[0] = %q, want either variant", got0)
	}
	if handles[0].SortedIndex != handles[1].SortedIndex {
		t.Fatal("expected both handles to share one cached *SortedIndex")
	}
}

func TestSortedIndexManagerSweepClosesIdleAndReopens(t *testing.T) {
	const ttl = time.Minute
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "report-files.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"k","v":"survives reap"}`})

	m := testSortedIndexManager(t, ttl)

	h, err := m.Acquire("report", []string{srcPath})
	if err != nil {
		t.Fatal(err)
	}
	h.Release()

	m.sweep(time.Now())
	if s := sortedIndexStatFor(t, m, "report"); !s.Open {
		t.Fatal("swept before IdleTTL elapsed")
	}

	m.sweep(time.Now().Add(ttl + time.Second))
	s := sortedIndexStatFor(t, m, "report")
	if s.Open {
		t.Fatal("expected idle sorted index to be closed")
	}
	if s.LastReapErr != nil {
		t.Fatalf("reap close failed: %v", s.LastReapErr)
	}

	if _, err := h.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed via stale handle, got %v", err)
	}

	// Reacquire reopens from the still-fresh on-disk cache (no rebuild
	// needed since the source hasn't changed).
	h2, err := m.Acquire("report", []string{srcPath})
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	got, err := h2.Get([]byte("k"))
	if err != nil || string(got) != `{"id":"k","v":"survives reap"}` {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}

func TestSortedIndexManagerRequiredOptions(t *testing.T) {
	if _, err := NewSortedIndexManager(SortedIndexManagerOptions{KeyFunc: JSONStringKey("id")}); err == nil {
		t.Error("missing CacheDir accepted")
	}
	if _, err := NewSortedIndexManager(SortedIndexManagerOptions{CacheDir: t.TempDir()}); err == nil {
		t.Error("missing KeyFunc accepted")
	}
}

func TestSortedIndexManagerAcquireRequiresSourcePaths(t *testing.T) {
	m := testSortedIndexManager(t, 0)
	if _, err := m.Acquire("report", nil); err == nil {
		t.Error("expected error for empty sourcePaths")
	}
}
