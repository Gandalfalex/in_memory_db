package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestBuildSortedIndexConcurrentCallsSameSidxPathDoNotCorrupt targets the
// gap buildLocks closes: two BuildSortedIndex calls racing on the same
// sidxPath used to be able to collide on shared, non-unique temp file
// names and corrupt each other's output. With the lock, they're
// serialized — both succeed, and whichever one's result ends up at
// sidxPath is a complete, internally consistent build from exactly one
// of the two, never a torn mix of both.
func TestBuildSortedIndexConcurrentCallsSameSidxPathDoNotCorrupt(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.jsonl")
	pathB := filepath.Join(dir, "b.jsonl")
	writeJSONLFile(t, pathA, []string{`{"id":"x","v":"a"}`, `{"id":"y","v":"a"}`})
	writeJSONLFile(t, pathB, []string{`{"id":"x","v":"b"}`, `{"id":"z","v":"b"}`})

	sidxPath := filepath.Join(dir, "shared.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = BuildSortedIndex(context.Background(), []string{pathA}, JSONStringKey("id"), sidxPath, opts)
	}()
	go func() {
		defer wg.Done()
		errs[1] = BuildSortedIndex(context.Background(), []string{pathB}, JSONStringKey("id"), sidxPath, opts)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("build[%d]: %v", i, err)
		}
	}

	si, err := OpenSortedIndex(sidxPath, JSONStringKey("id"))
	if err != nil {
		t.Fatalf("OpenSortedIndex after concurrent builds: %v (a corrupt file would fail checksum verification here)", err)
	}
	defer si.Close()

	if si.Len() != 2 {
		t.Fatalf("Len() = %d, want 2 (a whole result from one build, not a corrupt mix of both)", si.Len())
	}
	got, err := si.Get([]byte("x"))
	if err != nil {
		t.Fatalf("Get(x): %v", err)
	}
	if s := string(got); s != `{"id":"x","v":"a"}` && s != `{"id":"x","v":"b"}` {
		t.Fatalf("Get(x) = %q, want a clean a or b variant, not garbled", got)
	}
}

// TestEnsureFreshConcurrentCallsSameSidxPathDoNotCorrupt is the same
// property through the more commonly used entry point: concurrent
// EnsureFresh calls (as SortedIndexManager.Acquire would trigger under
// concurrent load) for a cold cache must not corrupt it either.
func TestEnsureFreshConcurrentCallsSameSidxPathDoNotCorrupt(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"x","v":1}`, `{"id":"y","v":1}`})

	sidxPath := filepath.Join(dir, "shared.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}

	var wg sync.WaitGroup
	results := make([]*SortedIndex, 4)
	errs := make([]error, 4)
	wg.Add(4)
	for i := range 4 {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = EnsureFresh(context.Background(), []string{srcPath}, sidxPath, JSONStringKey("id"), opts)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("EnsureFresh[%d]: %v", i, err)
		}
		defer results[i].Close()
		if got := results[i].Len(); got != 2 {
			t.Fatalf("EnsureFresh[%d].Len() = %d, want 2", i, got)
		}
	}
}

// cancelAfterCalls is a context.Context whose Err() reports
// context.Canceled starting from its Nth call, deterministically — used
// to pin exactly when a periodic ctx.Err() check inside a scan/merge
// loop observes cancellation, without racing wall-clock time against how
// fast the loop runs (which a real context.WithCancel + a goroutine that
// sleeps-then-cancels would require, and which could flake on a fast
// enough machine or a slow enough one).
type cancelAfterCalls struct {
	context.Context
	remaining int
}

func (c *cancelAfterCalls) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestBuildSortedIndexRespectsAlreadyCancelledContext(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := BuildSortedIndex(ctx, []string{srcPath}, JSONStringKey("id"), sidxPath, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildSortedIndex with pre-cancelled ctx = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(sidxPath); !os.IsNotExist(statErr) {
		t.Fatal("expected no sidx file to be left behind by a cancelled build")
	}
}

// TestBuildSortedIndexCancelMidScan exercises the periodic check inside
// scanOneSource's loop (not just the one-shot check at the top of
// buildSortedIndexLocked), so the dataset must be large enough to reach
// at least one ctxCheckInterval boundary.
func TestBuildSortedIndexCancelMidScan(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")

	const n = ctxCheckInterval + 100
	var buf bytes.Buffer
	buf.Grow(n * 24)
	for i := range n {
		fmt.Fprintf(&buf, `{"id":"key-%08d","v":1}`+"\n", i)
	}
	if err := os.WriteFile(srcPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	sidxPath := filepath.Join(dir, "index.sidx")
	// Call 1: buildSortedIndexLocked's entry check — must return nil so
	// the build actually starts. Call 2: the first periodic check inside
	// scanOneSource's loop, at line ctxCheckInterval — report cancelled.
	ctx := &cancelAfterCalls{Context: context.Background(), remaining: 2}

	err := BuildSortedIndex(ctx, []string{srcPath}, JSONStringKey("id"), sidxPath, SortedIndexOptions{ChunkEntries: 10_000, SparseInterval: 4096})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildSortedIndex = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(sidxPath); !os.IsNotExist(statErr) {
		t.Fatal("expected no sidx file left behind by a cancelled build")
	}
	// Temp run files must not leak either — same cleanup path as any
	// other build error.
	matches, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("leaked temp files after cancelled build: %v", matches)
	}
}
