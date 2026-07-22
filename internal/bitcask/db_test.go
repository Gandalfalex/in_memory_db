package bitcask

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	opts := DefaultOptions(t.TempDir())
	opts.SegmentSize = 4096 // small, to exercise rotation quickly in tests
	return opts
}

func TestPutGetDeleteRoundtrip(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "world" {
		t.Fatalf("got %q", got)
	}

	has, err := db.Has([]byte("hello"))
	if err != nil || !has {
		t.Fatalf("expected Has=true, err=%v", err)
	}

	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("hello")); err != kvtypes.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	has, err = db.Has([]byte("hello"))
	if err != nil || has {
		t.Fatalf("expected Has=false after delete, err=%v", err)
	}
}

func TestOverwrite(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2-longer-value"))
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2-longer-value" {
		t.Fatalf("got %q", got)
	}
}

func TestEmptyValue(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte{}); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty value, got %q", got)
	}
}

func TestSegmentRotation(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 2000
	val := make([]byte, 50)
	for i := range n {
		key := fmt.Sprintf("key-%05d", i)
		if err := db.Put([]byte(key), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	db.mu.RLock()
	numSegments := len(db.segments)
	db.mu.RUnlock()
	if numSegments < 2 {
		t.Fatalf("expected rotation to produce multiple segments, got %d", numSegments)
	}
	for i := range n {
		key := fmt.Sprintf("key-%05d", i)
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if len(got) != len(val) {
			t.Fatalf("get %d: wrong length %d", i, len(got))
		}
	}
}

func TestCloseReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := range n {
		key := fmt.Sprintf("k-%04d", i)
		db.Put([]byte(key), fmt.Appendf(nil, "v-%04d", i))
	}
	db.Delete([]byte("k-0005"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := range n {
		key := fmt.Sprintf("k-%04d", i)
		if key == "k-0005" {
			if _, err := db2.Get([]byte(key)); err != kvtypes.ErrNotFound {
				t.Fatalf("expected deleted key gone after reopen, got err=%v", err)
			}
			continue
		}
		got, err := db2.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		want := fmt.Sprintf("v-%04d", i)
		if string(got) != want {
			t.Fatalf("get %q after reopen: want %q got %q", key, want, got)
		}
	}
}

func TestCloseReopenWithSnapshot(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096
	opts.SnapshotOnClose = true

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		db.Put(fmt.Appendf(nil, "s-%04d", i), []byte("value"))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := filepathGlobOne(filepath.Join(dir, "index.snapshot")); err != nil {
		t.Fatalf("expected snapshot file to exist: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := range 500 {
		key := fmt.Sprintf("s-%04d", i)
		got, err := db2.Get([]byte(key))
		if err != nil || string(got) != "value" {
			t.Fatalf("get %q after snapshot reopen: got %q err=%v", key, got, err)
		}
	}
}

func TestCompactionReclaimsAndPreservesData(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096
	opts.CompactionRatio = 0.3

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Fill several segments, then overwrite most keys repeatedly to build
	// garbage in the earlier segments.
	const n = 300
	for i := range n {
		db.Put(fmt.Appendf(nil, "c-%04d", i), []byte("v1-------------"))
	}
	for round := range 5 {
		for i := range n {
			if i%3 != 0 {
				db.Put(fmt.Appendf(nil, "c-%04d", i), fmt.Appendf(nil, "v%d-------------", round))
			}
		}
	}

	db.mu.RLock()
	segsBefore := len(db.segments)
	db.mu.RUnlock()

	db.compactNow()

	db.mu.RLock()
	segsAfter := len(db.segments)
	db.mu.RUnlock()
	if segsAfter >= segsBefore {
		t.Fatalf("expected compaction to reduce segment count: before=%d after=%d", segsBefore, segsAfter)
	}

	for i := range n {
		key := fmt.Sprintf("c-%04d", i)
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q after compaction: %v", key, err)
		}
		if len(got) == 0 {
			t.Fatalf("get %q after compaction: empty value", key)
		}
	}
}

func filepathGlobOne(path string) (string, error) {
	matches, err := filepath.Glob(path)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no match for %s", path)
	}
	return matches[0], nil
}
