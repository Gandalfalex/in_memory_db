package kv

import (
	"errors"
	"fmt"
	"testing"
)

func TestKeyValidationSentinels(t *testing.T) {
	opts := testOptions(t)
	opts.SegmentSize = 1 << 20 // room for a MaxKeyLen-sized record
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put empty key: got %v, want ErrEmptyKey", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Get empty key: got %v, want ErrEmptyKey", err)
	}
	if _, err := db.Has(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Has empty key: got %v, want ErrEmptyKey", err)
	}
	if err := db.Delete(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete empty key: got %v, want ErrEmptyKey", err)
	}

	big := make([]byte, MaxKeyLen+1)
	if err := db.Put(big, []byte("v")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Put oversized key: got %v, want ErrKeyTooLarge", err)
	}
	if err := db.Delete(big); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Delete oversized key: got %v, want ErrKeyTooLarge", err)
	}
	if err := db.Put(make([]byte, MaxKeyLen), []byte("v")); err != nil {
		t.Fatalf("Put key of exactly MaxKeyLen: %v", err)
	}
}

func TestIteratorErrNilOnExhaustion(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	it := db.Iterator(IterOptions{})
	defer it.Close()
	for it.Next() {
	}
	if it.Err() != nil {
		t.Fatalf("exhausted iterator: Err=%v, want nil", it.Err())
	}
}

func TestDBAllRange(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 100
	for i := range n {
		db.Put(fmt.Appendf(nil, "all-%03d", i), []byte("v"))
	}
	seen := 0
	for k, v := range db.All(IterOptions{Prefix: []byte("all-")}) {
		if len(k) == 0 || string(v) != "v" {
			t.Fatalf("unexpected pair %q=%q", k, v)
		}
		seen++
	}
	if seen != n {
		t.Fatalf("ranged over %d keys, want %d", seen, n)
	}

	// early break must not panic or leak
	for range db.All(IterOptions{}) {
		break
	}
}

func TestBucketIteratorTypedAndStripped(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users := NewBucket[user](db, "users:", JSONCodec[user]{})
	users.Put("alice", user{Name: "Alice", Age: 30})
	users.Put("bob", user{Name: "Bob", Age: 40})
	// neighbor bucket must not leak in
	other := NewBucket[user](db, "teams:", JSONCodec[user]{})
	other.Put("x", user{Name: "nope"})

	got := map[string]user{}
	it := users.Iterator("")
	defer it.Close()
	for it.Next() {
		got[it.Key()] = it.Value()
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if len(got) != 2 || got["alice"].Age != 30 || got["bob"].Age != 40 {
		t.Fatalf("got %+v", got)
	}

	seen := 0
	for k, v := range users.All("") {
		if got[k] != v {
			t.Fatalf("All mismatch at %q: %+v", k, v)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("All yielded %d entries, want 2", seen)
	}
}

func TestBucketIteratorDecodeError(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users := NewBucket[user](db, "users:", JSONCodec[user]{})
	db.Put([]byte("users:broken"), []byte("{not json"))

	it := users.Iterator("")
	defer it.Close()
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("expected decode error, got nil")
	}
}

func TestStats(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := db.Stats()
	if s.Keys != 0 || s.Segments == 0 {
		t.Fatalf("fresh db stats: %+v", s)
	}
	db.Put([]byte("k1"), []byte("v1"))
	db.Put([]byte("k2"), []byte("v2"))
	db.Put([]byte("k1"), []byte("v1b")) // supersedes -> dead bytes
	s = db.Stats()
	if s.Keys != 2 {
		t.Fatalf("Keys=%d, want 2", s.Keys)
	}
	if s.DeadBytes <= 0 {
		t.Fatalf("DeadBytes=%d, want > 0 after overwrite", s.DeadBytes)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Stats().LastCompactionErr; err != nil {
		t.Fatalf("LastCompactionErr=%v after successful Compact, want nil", err)
	}
}

func TestSyncAndCompactExported(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("k"), []byte("v"))
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != ErrClosed {
		t.Fatalf("Sync after close: got %v, want ErrClosed", err)
	}
	if err := db.Compact(); err != ErrClosed {
		t.Fatalf("Compact after close: got %v, want ErrClosed", err)
	}
}

func TestOpenRejectsBadCompactionRatio(t *testing.T) {
	opts := testOptions(t)
	opts.CompactionRatio = 1.5
	if _, err := Open(opts); err == nil {
		t.Fatal("expected error for CompactionRatio > 1")
	}
}
