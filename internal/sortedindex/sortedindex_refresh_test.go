package sortedindex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gandalfalex/in_memory_db/internal/fileindex"
)

func TestIncrementalEligible(t *testing.T) {
	base := sourceStat{path: "base.jsonl", size: 100, modTime: 1}
	changes := sourceStat{path: "changes.jsonl", size: 50, modTime: 2}
	newFile := sourceStat{path: "new.jsonl", size: 10, modTime: 3}

	cases := []struct {
		name     string
		recorded []sourceStat
		current  []sourceStat
		wantOK   bool
		wantNew  int
	}{
		{"one new source appended", []sourceStat{base}, []sourceStat{base, changes}, true, 1},
		{"two new sources appended", []sourceStat{base}, []sourceStat{base, changes, newFile}, true, 2},
		{"nothing changed, same length", []sourceStat{base, changes}, []sourceStat{base, changes}, false, 0},
		{"existing source's size changed", []sourceStat{base}, []sourceStat{{path: base.path, size: 999, modTime: base.modTime}, changes}, false, 0},
		{"existing source's mtime changed", []sourceStat{base}, []sourceStat{{path: base.path, size: base.size, modTime: 999}, changes}, false, 0},
		{"a source was removed", []sourceStat{base, changes}, []sourceStat{base}, false, 0},
		{"sources reordered", []sourceStat{base, changes}, []sourceStat{changes, base}, false, 0},
		{"empty recorded, first-ever state", nil, []sourceStat{base}, true, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			newSrcs, ok := incrementalEligible(c.recorded, c.current)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if ok && len(newSrcs) != c.wantNew {
				t.Fatalf("len(newSources) = %d, want %d", len(newSrcs), c.wantNew)
			}
		})
	}
}

func TestEnsureFreshIncrementalRefreshAppendsNewSource(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	writeJSONLFile(t, basePath, []string{
		`{"id":"a","status":"pending"}`,
		`{"id":"b","status":"pending"}`,
	})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}

	si1, err := EnsureFresh(context.Background(), []string{basePath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si1.Close()

	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, changesPath, []string{
		`{"id":"a","status":"done"}`, // overrides base's "a"
		`{"id":"c","status":"pending"}`,
	})

	si2, err := EnsureFresh(context.Background(), []string{basePath, changesPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatalf("EnsureFresh (incremental): %v", err)
	}
	defer si2.Close()

	if si2.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", si2.Len())
	}
	want := map[string]string{
		"a": `{"id":"a","status":"done"}`,
		"b": `{"id":"b","status":"pending"}`,
		"c": `{"id":"c","status":"pending"}`,
	}
	for id, w := range want {
		got, err := si2.Get([]byte(id))
		if err != nil || string(got) != w {
			t.Fatalf("Get(%s) = %q, %v, want %q", id, got, err, w)
		}
	}
}

// The strongest proof the incremental path never re-reads an already
// recorded source: corrupt basePath's *content* after the first build
// while keeping its size and mtime byte-for-byte identical to what's
// already recorded (so incrementalEligible still says "unchanged"). If
// the incremental refresh ever re-scanned basePath, JSONStringKey would
// reject every garbage line, and "a"/"b" would come back ErrNotFound
// instead of their original values.
// TestRefreshSortedIndexNeverReopensExistingSources is a white-box proof
// (calls the unexported refreshSortedIndexLocked directly, bypassing
// EnsureFresh's own statSources call — which would itself fail on a
// missing recorded source, for an unrelated reason: it needs every
// current source's stat to decide what to do, not to build) that the
// actual merge machinery never reopens an already-recorded source: after
// the initial build, basePath is deleted entirely, and the refresh still
// succeeds — it only ever reads the existing sidx (for prior entries)
// and the new source (for new ones).
//
// (A black-box version of this test — corrupt basePath's bytes at the
// same size/mtime and check Get still returns the original content —
// isn't actually possible: SortedIndex entries are live pointers into
// the source file, not copies, so Get always reads whatever's currently
// at that offset regardless of whether the index was built incrementally
// or from scratch. That's inherent to the whole package's zero-copy
// design, not something incremental refresh changes.)
func TestRefreshSortedIndexNeverReopensExistingSources(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":1}`, `{"id":"b","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	if err := BuildSortedIndex(context.Background(), []string{basePath}, fileindex.JSONStringKey("id"), sidxPath, opts); err != nil {
		t.Fatal(err)
	}
	recorded, err := readSourcesFile(sourcesPathFor(sidxPath))
	if err != nil {
		t.Fatal(err)
	}

	// basePath is now gone entirely — any code path that tries to reopen
	// it (a full rebuild, or a broken "incremental" implementation) fails
	// immediately.
	if err := os.Remove(basePath); err != nil {
		t.Fatal(err)
	}

	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, changesPath, []string{`{"id":"c","v":1}`})
	newSrcs, err := statSources([]string{changesPath})
	if err != nil {
		t.Fatal(err)
	}

	if err := refreshSortedIndexLocked(context.Background(), recorded, newSrcs, sidxPath, fileindex.JSONStringKey("id"), opts); err != nil {
		t.Fatalf("refreshSortedIndexLocked with a removed prior source: %v (this would only fail if it tried to reopen basePath)", err)
	}
}

func TestEnsureFreshIncrementalRefreshMultipleNewSources(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":"base"}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	si1, err := EnsureFresh(context.Background(), []string{basePath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si1.Close()

	change1 := filepath.Join(dir, "change1.jsonl")
	change2 := filepath.Join(dir, "change2.jsonl")
	writeJSONLFile(t, change1, []string{`{"id":"a","v":"change1"}`})
	writeJSONLFile(t, change2, []string{`{"id":"a","v":"change2"}`})

	si2, err := EnsureFresh(context.Background(), []string{basePath, change1, change2}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatalf("EnsureFresh (incremental, two new sources at once): %v", err)
	}
	defer si2.Close()

	got, err := si2.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":"change2"}` {
		t.Fatalf("Get(a) = %q, %v, want change2 (highest precedence)", got, err)
	}
}

// A corrupt existing sidx must fail the incremental path loudly, not
// silently produce a wrong or incomplete result — same "never trust
// unverified bytes" posture as everywhere else in this package.
func TestEnsureFreshIncrementalRefreshFailsOnCorruptExistingSidx(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	si1, err := EnsureFresh(context.Background(), []string{basePath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si1.Close()

	// Flip a byte in the entries region so the checksum no longer matches.
	data, err := os.ReadFile(sidxPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-5] ^= 0xFF
	if err := os.WriteFile(sidxPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, changesPath, []string{`{"id":"b","v":1}`})

	_, err = EnsureFresh(context.Background(), []string{basePath, changesPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err == nil {
		t.Fatal("expected an error from a corrupt existing sidx, not a silently wrong result")
	}
}

// EnsureFresh's incremental path (the same as its full-rebuild path)
// must respect ctx cancellation.
func TestEnsureFreshIncrementalRefreshRespectsCancelledContext(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	si1, err := EnsureFresh(context.Background(), []string{basePath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si1.Close()

	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, changesPath, []string{`{"id":"b","v":1}`})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = EnsureFresh(ctx, []string{basePath, changesPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureFresh with pre-cancelled ctx = %v, want context.Canceled", err)
	}
}
