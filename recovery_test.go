package kv

import (
	"fmt"
	"testing"
)

// simulateCrash closes every segment file directly, bypassing Close()'s
// snapshot step, so the next Open must fall back to (or partially rely on)
// a full segment scan rather than trusting a checkpoint.
func simulateCrash(t *testing.T, db *DB) {
	t.Helper()
	db.stopCompactor()
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, seg := range db.segments {
		if err := seg.close(); err != nil {
			t.Fatalf("close segment %d: %v", seg.id, err)
		}
	}
}

func TestRecoveryWithoutCleanClose(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("r-%04d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	simulateCrash(t, db)

	if _, _, err := loadSnapshotInto(dir, newIndex()); err == nil {
		t.Fatal("expected no usable snapshot to exist after simulated crash")
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("r-%04d", i)
		got, err := db2.Get([]byte(key))
		if err != nil || string(got) != "value" {
			t.Fatalf("get %q after crash recovery: got %q err=%v", key, got, err)
		}
	}
}

func TestRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 65536

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("t-%02d", i)), []byte("committed-value")); err != nil {
			t.Fatal(err)
		}
	}

	db.stopCompactor()
	db.mu.Lock()
	active := db.active
	goodSize := active.size

	// Craft a full, valid record as if the next write were about to
	// happen, then only persist a truncated prefix of its value bytes —
	// simulating a crash mid-append, before segment.size (and the index)
	// were ever updated to reflect it.
	record := encodeRecord([]byte("t-torn"), []byte("this-value-never-fully-lands-on-disk"), false, 1)
	truncated := len(record) - 5
	copy(active.mf.Bytes()[goodSize:goodSize+int64(truncated)], record[:truncated])

	for _, seg := range db.segments {
		if err := seg.close(); err != nil {
			t.Fatalf("close segment %d: %v", seg.id, err)
		}
	}
	db.mu.Unlock()

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("t-%02d", i)
		got, err := db2.Get([]byte(key))
		if err != nil || string(got) != "committed-value" {
			t.Fatalf("get %q: got %q err=%v", key, got, err)
		}
	}
	if _, err := db2.Get([]byte("t-torn")); err != ErrNotFound {
		t.Fatalf("expected torn record to be dropped, got err=%v", err)
	}
}

// TestRecoveryCorruptRecordTruncatesLog confirms the documented policy: a
// checksum failure anywhere in a segment truncates recovery of that
// segment at that point (everything from there on is untrusted), rather
// than erroring Open() entirely or skipping just the one bad record and
// continuing to scan past it.
func TestRecoveryCorruptRecordTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 65536

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("g-%02d", i)), []byte("good")); err != nil {
			t.Fatal(err)
		}
	}

	db.stopCompactor()
	db.mu.Lock()
	dst := db.active.mf.Bytes()
	// Flip a byte inside the very first record's value, invalidating its
	// stored CRC.
	dst[headerSize+len("g-00")] ^= 0xFF
	for _, seg := range db.segments {
		if err := seg.close(); err != nil {
			t.Fatalf("close segment %d: %v", seg.id, err)
		}
	}
	db.mu.Unlock()

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Open must not fail outright on a corrupt record: %v", err)
	}
	defer db2.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("g-%02d", i)
		if _, err := db2.Get([]byte(key)); err != ErrNotFound {
			t.Fatalf("expected key %q to be dropped after corruption truncation, err=%v", key, err)
		}
	}
}
