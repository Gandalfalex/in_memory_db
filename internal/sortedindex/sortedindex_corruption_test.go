package sortedindex

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gandalfalex/in_memory_db/internal/fileindex"
)

// buildValidSortedIndex builds (without opening) a small valid index and
// returns its sidx path, for tests that corrupt one sidecar file and then
// attempt to Open.
func buildValidSortedIndex(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.jsonl")
	writeJSONLFile(t, srcPath, []string{`{"id":"a","v":1}`, `{"id":"b","v":1}`})

	sidxPath := filepath.Join(dir, "data.sidx")
	if err := BuildSortedIndex(context.Background(), []string{srcPath}, fileindex.JSONStringKey("id"), sidxPath, SortedIndexOptions{ChunkEntries: 2, SparseInterval: 2}); err != nil {
		t.Fatalf("BuildSortedIndex: %v", err)
	}
	return sidxPath
}

// corruptFile overwrites path's content, exercising the read side's
// error handling — bad magic, wrong version, truncation, or a checksum
// mismatch — for whichever sidecar format is under test.
func corruptFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSortedIndexCorruptSidx(t *testing.T) {
	cases := map[string][]byte{
		"bad magic": []byte("XXXX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
		"truncated": []byte("KV"),
		"empty":     {},
		// valid header (version 1, count 0, no entries) but a trailer that
		// doesn't match crc32 of zero entry bytes (which is 0, not 0xFF..FF)
		"bad crc": append([]byte(sidxMagic), []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}...),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			sidxPath := buildValidSortedIndex(t)
			corruptFile(t, sidxPath, content)
			if _, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id")); err == nil {
				t.Fatal("expected error opening a corrupt sidx file")
			}
		})
	}
}

func TestSortedIndexCorruptSparse(t *testing.T) {
	cases := map[string][]byte{
		"bad magic": []byte("XXXX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
		"truncated": []byte("K"),
		"empty":     {},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			sidxPath := buildValidSortedIndex(t)
			corruptFile(t, sparsePathFor(sidxPath), content)
			if _, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id")); err == nil {
				t.Fatal("expected error opening with a corrupt sparse sidecar")
			}
		})
	}
}

func TestSortedIndexCorruptBloom(t *testing.T) {
	cases := map[string][]byte{
		"bad magic": []byte("XXXX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
		"truncated": []byte("K"),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			sidxPath := buildValidSortedIndex(t)
			corruptFile(t, bloomPathFor(sidxPath), content)
			if _, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id")); err == nil {
				t.Fatal("expected error opening with a corrupt bloom sidecar")
			}
		})
	}
}

func TestSortedIndexCorruptSources(t *testing.T) {
	cases := map[string][]byte{
		"bad magic": []byte("XXXX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
		"truncated": []byte("K"),
		"empty":     {},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			sidxPath := buildValidSortedIndex(t)
			corruptFile(t, sourcesPathFor(sidxPath), content)
			if _, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id")); err == nil {
				t.Fatal("expected error opening with a corrupt sources sidecar")
			}
		})
	}
}

// A missing (never built) bloom sidecar is not corruption — Open degrades
// gracefully, per readBloomFile's documented (nil, nil) contract.
func TestSortedIndexMissingBloomSidecarIsNotAnError(t *testing.T) {
	sidxPath := buildValidSortedIndex(t)
	if err := os.Remove(bloomPathFor(sidxPath)); err != nil {
		t.Fatal(err)
	}
	si, err := OpenSortedIndex(sidxPath, fileindex.JSONStringKey("id"))
	if err != nil {
		t.Fatalf("OpenSortedIndex with no bloom sidecar: %v", err)
	}
	defer si.Close()
	if si.bloom != nil {
		t.Fatal("expected nil bloom filter when sidecar is absent")
	}
	got, err := si.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":1}` {
		t.Fatalf("Get(a) = %q, %v", got, err)
	}
}

// FuzzReadSidxEntry targets the binary sidx entry parser directly: it
// reads whatever bytes are on disk at a computed offset with no
// higher-level validation before it (Get/Prefix/the incremental refresh
// merge all call it straight off a bufio.Reader), so truncation or
// corruption landing mid-entry must produce a clean error, never a
// panic — a length field taken from untrusted bytes and then used to
// size a read/allocation is exactly the classic parser-panic shape.
func FuzzReadSidxEntry(f *testing.F) {
	var valid bytes.Buffer
	if _, err := writeSidxEntry(&valid, sortEntry{key: []byte("some-key"), fileIdx: 3, srcOff: 12345, length: 678}); err != nil {
		f.Fatal(err)
	}
	f.Add(valid.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 0})                    // keyLen=0, nothing else
	f.Add([]byte{0xFF, 0xFF})              // keyLen=65535, no key bytes follow
	f.Add([]byte{0, 3, 'a', 'b'})          // keyLen=3 but only 2 key bytes
	f.Add(append([]byte{0, 1, 'k'}, 0, 0)) // key ok, truncated fileIdx

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		_, _ = readSidxEntry(r) // must not panic regardless of input
	})
}
