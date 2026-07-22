package sortedindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Gandalfalex/in_memory_db/internal/fileindex"
	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
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

func buildTestSortedIndex(t *testing.T, lines []string, opts SortedIndexOptions) (*SortedIndex, string) {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, lines)

	sidxPath := filepath.Join(dir, "data.sidx")
	if err := BuildSortedIndex(context.Background(), []string{srcPath}, fileindex.JSONStringKey("id"), sidxPath, opts); err != nil {
		t.Fatalf("BuildSortedIndex: %v", err)
	}

	si, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id"))
	if err != nil {
		t.Fatalf("OpenSortedIndex: %v", err)
	}
	t.Cleanup(func() { si.Close() })
	return si, sidxPath
}

func TestSortedIndexGetAndLastLineWins(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"b","v":1}`,
		`{"id":"a","v":1}`,
		`{"id":"c","v":1}`,
		`{"id":"a","v":2}`, // duplicate key, later source offset must win
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	if got := si.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}

	line, err := si.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if string(line) != `{"id":"a","v":2}` {
		t.Fatalf("Get(a) = %q, want v:2 (last-write-wins)", line)
	}

	if _, err := si.Get([]byte("nope")); !errors.Is(err, kvtypes.ErrNotFound) {
		t.Fatalf("Get(nope) = %v, want ErrNotFound", err)
	}

	if _, err := si.Get(nil); !errors.Is(err, kvtypes.ErrEmptyKey) {
		t.Fatalf("Get(nil) = %v, want ErrEmptyKey", err)
	}
}

func TestSortedIndexGetSpansMultipleChunksAndRuns(t *testing.T) {
	// ChunkEntries=1 forces one run file per line, exercising the k-way
	// merge (not just an in-RAM sort of a single chunk).
	var lines []string
	want := map[string]string{}
	for _, id := range []string{"m", "a", "z", "c", "y", "b"} {
		l := `{"id":"` + id + `","v":1}`
		lines = append(lines, l)
		want[id] = l
	}
	si, _ := buildTestSortedIndex(t, lines, SortedIndexOptions{ChunkEntries: 1, SparseInterval: 2})

	for id, wantLine := range want {
		got, err := si.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if string(got) != wantLine {
			t.Fatalf("Get(%s) = %q, want %q", id, got, wantLine)
		}
	}
}

func TestSortedIndexAllIsSorted(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"b","v":1}`,
		`{"id":"a","v":1}`,
		`{"id":"c","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	var order []string
	for k := range si.All() {
		order = append(order, string(k))
	}
	want := []string{"a", "b", "c"}
	if len(order) != len(want) {
		t.Fatalf("All() = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("All() = %v, want %v", order, want)
		}
	}
}

func TestSortedIndexPrefix(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"apple","v":1}`,
		`{"id":"banana","v":1}`,
		`{"id":"apricot","v":1}`,
		`{"id":"avocado","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	var got []string
	for k := range si.Prefix([]byte("ap")) {
		got = append(got, string(k))
	}
	want := []string{"apple", "apricot"} // sorted order between "ap" and "aq"
	if len(got) != len(want) {
		t.Fatalf("Prefix(ap) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Prefix(ap) = %v, want %v", got, want)
		}
	}
}

func TestSortedIndexFilterAll(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"a","v":1}`,
		`{"id":"b","v":9}`,
		`{"id":"c","v":1}`,
		`{"id":"d","v":9}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	var got []string
	for k := range FilterAll(si.All(), func(line []byte) bool { return bytes.Contains(line, []byte(`"v":9`)) }) {
		got = append(got, string(k))
	}
	want := []string{"b", "d"}
	if len(got) != len(want) {
		t.Fatalf("FilterAll v:9 = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FilterAll v:9 = %v, want %v", got, want)
		}
	}
}

func TestSortedIndexBloomRejectsAbsentKeys(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"only","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2, BloomFPR: 0.001})

	if si.bloom == nil {
		t.Fatal("expected a Bloom filter to be built with BloomFPR set")
	}
	if _, err := si.Get([]byte("definitely-absent")); !errors.Is(err, kvtypes.ErrNotFound) {
		t.Fatalf("Get(definitely-absent) = %v, want ErrNotFound", err)
	}
}

func TestSortedIndexBloomOptOut(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"only","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2, BloomFPR: -1})

	if si.bloom != nil {
		t.Fatal("expected no Bloom filter with BloomFPR < 0")
	}
	got, err := si.Get([]byte("only"))
	if err != nil || string(got) != `{"id":"only","v":1}` {
		t.Fatalf("Get(only) = %q, %v", got, err)
	}
}

// A false positive from the Bloom filter just costs an extra bounded scan
// (harmless); a false negative would silently return ErrNotFound for a
// key that actually exists — the one failure mode that must never happen.
// Build enough keys that a bit-indexing bug (off-by-one, k=0, etc.) would
// show up as at least one miss.
func TestSortedIndexBloomNeverFalseNegative(t *testing.T) {
	const n = 2000
	lines := make([]string, n)
	keys := make([]string, n)
	for i := range n {
		id := fmt.Sprintf("key-%05d", i)
		keys[i] = id
		lines[i] = fmt.Sprintf(`{"id":"%s","v":%d}`, id, i)
	}
	si, _ := buildTestSortedIndex(t, lines, SortedIndexOptions{ChunkEntries: 500, SparseInterval: 64, BloomFPR: 0.01})
	if si.bloom == nil {
		t.Fatal("expected a Bloom filter to be built")
	}
	for _, k := range keys {
		if !si.bloom.mayContain([]byte(k)) {
			t.Fatalf("bloom false negative for present key %q", k)
		}
		if _, err := si.Get([]byte(k)); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}
}

// TestSortedIndexConcurrentCloseDuringScan exercises the per-entry lock
// scoping in Get/nextPrefixMatch (see SortedIndex.mu's doc comment): a
// scan running concurrently with Close must never panic or data-race
// (checked by -race), and Close must still take effect regardless of
// which interleaving happens.
func TestSortedIndexConcurrentCloseDuringScan(t *testing.T) {
	const n = 5000
	lines := make([]string, n)
	for i := range n {
		lines[i] = fmt.Sprintf(`{"id":"key-%05d","v":%d}`, i, i)
	}
	si, _ := buildTestSortedIndex(t, lines, SortedIndexOptions{ChunkEntries: 500, SparseInterval: 16})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for range si.All() {
		}
	}()
	go func() {
		defer wg.Done()
		for i := range n {
			si.Get(fmt.Appendf(nil, "key-%05d", i)) // result irrelevant; must not panic or race
		}
	}()
	go func() {
		defer wg.Done()
		si.Close()
	}()
	wg.Wait()

	if _, err := si.Get([]byte("key-00000")); !errors.Is(err, kvtypes.ErrClosed) {
		t.Fatalf("Get after concurrent Close settled = %v, want ErrClosed", err)
	}
}

func TestSortedIndexEmptyIndex(t *testing.T) {
	// No line here has a field fileindex.JSONStringKey("id") accepts.
	si, _ := buildTestSortedIndex(t, []string{`not json`, ``, `{"novalidid":1}`}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	if si.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", si.Len())
	}
	if _, err := si.Get([]byte("anything")); !errors.Is(err, kvtypes.ErrNotFound) {
		t.Fatalf("Get on empty index = %v, want ErrNotFound", err)
	}
	if has, err := si.Has([]byte("anything")); err != nil || has {
		t.Fatalf("Has on empty index = %v, %v, want false, nil", has, err)
	}
	count := 0
	for range si.All() {
		count++
	}
	if count != 0 {
		t.Fatalf("All() on empty index yielded %d entries, want 0", count)
	}
}

func TestSortedIndexSparseIntervalExtremes(t *testing.T) {
	lines := []string{
		`{"id":"a","v":1}`, `{"id":"b","v":1}`, `{"id":"c","v":1}`,
		`{"id":"d","v":1}`, `{"id":"e","v":1}`,
	}
	t.Run("interval=1, every entry sampled", func(t *testing.T) {
		si, _ := buildTestSortedIndex(t, lines, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 1})
		for _, id := range []string{"a", "b", "c", "d", "e"} {
			if _, err := si.Get([]byte(id)); err != nil {
				t.Fatalf("Get(%s): %v", id, err)
			}
		}
	})
	t.Run("interval larger than dataset, only first sampled", func(t *testing.T) {
		si, _ := buildTestSortedIndex(t, lines, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 1000})
		for _, id := range []string{"a", "b", "c", "d", "e"} {
			if _, err := si.Get([]byte(id)); err != nil {
				t.Fatalf("Get(%s): %v", id, err)
			}
		}
		if _, err := si.Get([]byte("nope")); !errors.Is(err, kvtypes.ErrNotFound) {
			t.Fatalf("Get(nope) = %v, want ErrNotFound", err)
		}
	})
}

func TestSortedIndexReopenReadsSidecars(t *testing.T) {
	si, sidxPath := buildTestSortedIndex(t, []string{
		`{"id":"a","v":1}`,
		`{"id":"b","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})
	si.Close()

	reopened, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got, err := reopened.Get([]byte("a")); err != nil || string(got) != `{"id":"a","v":1}` {
		t.Fatalf("Get(a) after reopen = %q, %v", got, err)
	}
}

func TestSortedIndexHas(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{
		`{"id":"a","v":1}`,
	}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})

	if has, err := si.Has([]byte("a")); err != nil || !has {
		t.Fatalf("Has(a) = %v, %v, want true, nil", has, err)
	}
	if has, err := si.Has([]byte("nope")); err != nil || has {
		t.Fatalf("Has(nope) = %v, %v, want false, nil", has, err)
	}
	if _, err := si.Has(nil); !errors.Is(err, kvtypes.ErrEmptyKey) {
		t.Fatalf("Has(nil) = %v, want ErrEmptyKey", err)
	}
}

func TestSortedIndexHasAfterClose(t *testing.T) {
	si, _ := buildTestSortedIndex(t, []string{`{"id":"a","v":1}`}, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2})
	si.Close()
	if _, err := si.Has([]byte("a")); !errors.Is(err, kvtypes.ErrClosed) {
		t.Fatalf("Has after Close = %v, want ErrClosed", err)
	}
}

// --- multi-source (base + change files) ----------------------------------

func TestSortedIndexMultiSourceLaterFileWins(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "report-files.jsonl")
	changesPath := filepath.Join(dir, "report-changes.jsonl")
	writeJSONLFile(t, basePath, []string{
		`{"id":"a","status":"pending"}`,
		`{"id":"b","status":"pending"}`,
		`{"id":"c","status":"pending"}`,
	})
	writeJSONLFile(t, changesPath, []string{
		`{"id":"b","status":"done"}`,
		`{"id":"a","status":"done"}`,
	})

	sidxPath := filepath.Join(dir, "cache", "index.sidx")
	if err := os.MkdirAll(filepath.Dir(sidxPath), 0o755); err != nil {
		t.Fatal(err)
	}
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	if err := BuildSortedIndex(context.Background(), []string{basePath, changesPath}, fileindex.JSONStringKey("id"), sidxPath, opts); err != nil {
		t.Fatalf("BuildSortedIndex: %v", err)
	}
	si, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id"))
	if err != nil {
		t.Fatalf("OpenSortedIndex: %v", err)
	}
	defer si.Close()

	if si.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", si.Len())
	}

	for id, want := range map[string]string{
		"a": `{"id":"a","status":"done"}`,    // overridden by changes file
		"b": `{"id":"b","status":"done"}`,    // overridden by changes file
		"c": `{"id":"c","status":"pending"}`, // untouched, still from base
	} {
		got, err := si.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%s) = %q, want %q", id, got, want)
		}
	}

	if got := si.SourcePaths(); len(got) != 2 || got[0] != basePath || got[1] != changesPath {
		t.Fatalf("SourcePaths() = %v, want [%s %s]", got, basePath, changesPath)
	}
}

// TestSortedIndexThreeSourcePrecedenceChain checks "later file wins" is
// transitive across a chain, not just correct pairwise: change2 must beat
// both change1 and base for key "a", change1 must beat base for key "b",
// and base must survive untouched for key "c".
func TestSortedIndexThreeSourcePrecedenceChain(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	change1Path := filepath.Join(dir, "change1.jsonl")
	change2Path := filepath.Join(dir, "change2.jsonl")
	writeJSONLFile(t, basePath, []string{
		`{"id":"a","v":"base"}`,
		`{"id":"b","v":"base"}`,
		`{"id":"c","v":"base"}`,
	})
	writeJSONLFile(t, change1Path, []string{
		`{"id":"a","v":"change1"}`,
		`{"id":"b","v":"change1"}`,
	})
	writeJSONLFile(t, change2Path, []string{
		`{"id":"a","v":"change2"}`,
	})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	if err := BuildSortedIndex(context.Background(), []string{basePath, change1Path, change2Path}, fileindex.JSONStringKey("id"), sidxPath, opts); err != nil {
		t.Fatal(err)
	}
	si, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id"))
	if err != nil {
		t.Fatal(err)
	}
	defer si.Close()

	want := map[string]string{
		"a": `{"id":"a","v":"change2"}`,
		"b": `{"id":"b","v":"change1"}`,
		"c": `{"id":"c","v":"base"}`,
	}
	for id, w := range want {
		got, err := si.Get([]byte(id))
		if err != nil || string(got) != w {
			t.Fatalf("Get(%s) = %q, %v, want %q", id, got, err, w)
		}
	}
}

// TestSortedIndexOpenClosesAlreadyOpenedSourcesOnFailure exercises
// OpenSortedIndex's cleanup path (closeAll): with 2+ sources, if a later
// one fails to open, the earlier ones it already opened must be closed
// before returning the error, not leaked.
func TestSortedIndexOpenClosesAlreadyOpenedSourcesOnFailure(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":1}`})
	writeJSONLFile(t, changesPath, []string{`{"id":"b","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	if err := BuildSortedIndex(context.Background(), []string{basePath, changesPath}, fileindex.JSONStringKey("id"), sidxPath, opts); err != nil {
		t.Fatal(err)
	}

	// Remove the second source after building: OpenSortedIndex will
	// successfully open basePath (index 0) before failing on changesPath
	// (index 1), forcing the partial-open cleanup path.
	if err := os.Remove(changesPath); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id")); err == nil {
		t.Fatal("expected error opening with a missing source file")
	}

	// The already-opened basePath fd must have been closed by the failed
	// Open's cleanup, not leaked — removing it here is just confirming
	// nothing about the failed attempt left the file locked/in a bad
	// state (unix delete would succeed either way; this is a smoke check
	// that the failure path completed cleanly, not a leak proof).
	if err := os.Remove(basePath); err != nil {
		t.Fatal(err)
	}
}

// --- EnsureFresh (stat-based rebuild-or-reopen) ---------------------------

func TestEnsureFreshBuildsOnceThenReusesCache(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}

	si1, err := EnsureFresh(context.Background(), []string{srcPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatalf("EnsureFresh (build): %v", err)
	}
	si1.Close()

	sourcesBefore, err := os.Stat(sourcesPathFor(sidxPath))
	if err != nil {
		t.Fatal(err)
	}

	si2, err := EnsureFresh(context.Background(), []string{srcPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatalf("EnsureFresh (reuse): %v", err)
	}
	defer si2.Close()

	sourcesAfter, err := os.Stat(sourcesPathFor(sidxPath))
	if err != nil {
		t.Fatal(err)
	}
	if !sourcesBefore.ModTime().Equal(sourcesAfter.ModTime()) {
		t.Fatal("EnsureFresh rebuilt when the source hadn't changed")
	}

	got, err := si2.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":1}` {
		t.Fatalf("Get(a) = %q, %v", got, err)
	}
}

func TestEnsureFreshRebuildsWhenSourceChanges(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}

	si1, err := EnsureFresh(context.Background(), []string{srcPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si1.Close()

	// Force a distinguishable mtime, then change the source's content.
	future := time.Now().Add(time.Hour)
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":2}`, `{"id":"b","v":1}`})
	if err := os.Chtimes(srcPath, future, future); err != nil {
		t.Fatal(err)
	}

	si2, err := EnsureFresh(context.Background(), []string{srcPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatalf("EnsureFresh (rebuild): %v", err)
	}
	defer si2.Close()

	if si2.Len() != 2 {
		t.Fatalf("Len() = %d, want 2 after rebuild", si2.Len())
	}
	got, err := si2.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":2}` {
		t.Fatalf("Get(a) after rebuild = %q, %v, want updated value", got, err)
	}
}

// A source file that was removed entirely (not just changed) must fail
// with a clear, wrapped os.ErrNotExist from the stat step, not a
// confusing error from deeper in the build/open machinery.
func TestEnsureFreshErrorsClearlyWhenSourceRemoved(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.jsonl")
	changesPath := filepath.Join(dir, "changes.jsonl")
	writeJSONLFile(t, basePath, []string{`{"id":"a","v":1}`})
	writeJSONLFile(t, changesPath, []string{`{"id":"b","v":1}`})

	sidxPath := filepath.Join(dir, "index.sidx")
	opts := SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}
	si, err := EnsureFresh(context.Background(), []string{basePath, changesPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err != nil {
		t.Fatal(err)
	}
	si.Close()

	if err := os.Remove(changesPath); err != nil {
		t.Fatal(err)
	}

	_, err = EnsureFresh(context.Background(), []string{basePath, changesPath}, sidxPath, fileindex.JSONStringKey("id"), opts)
	if err == nil {
		t.Fatal("expected error when a source file no longer exists")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected a wrapped os.ErrNotExist, got %v", err)
	}
}
