package kv

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestIndexPutGet(t *testing.T) {
	idx := newIndex()
	if _, hadPrev := idx.put([]byte("a"), location{1, 10, 5}); hadPrev {
		t.Fatal("expected new key")
	}
	loc, found := idx.get([]byte("a"))
	if !found || loc != (location{1, 10, 5}) {
		t.Fatalf("got %+v found=%v", loc, found)
	}
	if _, found := idx.get([]byte("missing")); found {
		t.Fatal("expected not found")
	}
}

func TestIndexOverwrite(t *testing.T) {
	idx := newIndex()
	idx.put([]byte("k"), location{1, 0, 5})
	prev, hadPrev := idx.put([]byte("k"), location{2, 100, 8})
	if !hadPrev || prev != (location{1, 0, 5}) {
		t.Fatalf("expected overwrite returning prev location, got %+v hadPrev=%v", prev, hadPrev)
	}
	if idx.len() != 1 {
		t.Fatalf("expected 1 live entry, got %d", idx.len())
	}
	loc, found := idx.get([]byte("k"))
	if !found || loc != (location{2, 100, 8}) {
		t.Fatalf("overwrite not applied: %+v found=%v", loc, found)
	}
}

func TestIndexDeleteThenReinsert(t *testing.T) {
	idx := newIndex()
	idx.put([]byte("k"), location{1, 0, 5})
	prev, hadPrev := idx.delete([]byte("k"))
	if !hadPrev || prev != (location{1, 0, 5}) {
		t.Fatalf("expected delete to report key was live, got prev=%+v hadPrev=%v", prev, hadPrev)
	}
	if _, found := idx.get([]byte("k")); found {
		t.Fatal("expected key gone after delete")
	}
	if idx.len() != 0 {
		t.Fatalf("expected 0 live entries, got %d", idx.len())
	}
	// Reinsert must reuse the tombstoned slot correctly, not treat it as a
	// pre-existing live collision.
	if _, hadPrev := idx.put([]byte("k"), location{3, 30, 3}); hadPrev {
		t.Fatal("expected reinsert to count as new key (no prev)")
	}
	loc, found := idx.get([]byte("k"))
	if !found || loc != (location{3, 30, 3}) {
		t.Fatalf("reinsert location wrong: %+v found=%v", loc, found)
	}
	if _, hadPrev := idx.delete([]byte("k")); !hadPrev {
		t.Fatal("second delete should find the live reinsert")
	}
	if _, hadPrev := idx.delete([]byte("k")); hadPrev {
		t.Fatal("third delete on already-tombstoned key should report no prev")
	}
}

func TestIndexResizeCorrectness(t *testing.T) {
	idx := newIndex()
	const n = 20000 // several rehashes past initialSlotCapacity=1024
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		loc := location{uint32(i), uint32(i * 7), uint32(i % 50)}
		if _, hadPrev := idx.put(key, loc); hadPrev {
			t.Fatalf("put %d: expected new key", i)
		}
	}
	if idx.len() != n {
		t.Fatalf("expected %d live entries, got %d", n, idx.len())
	}
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		loc, found := idx.get(key)
		if !found {
			t.Fatalf("key %d missing after resize", i)
		}
		want := location{uint32(i), uint32(i * 7), uint32(i % 50)}
		if loc != want {
			t.Fatalf("key %d corrupted after resize: got %+v want %+v", i, loc, want)
		}
	}
}

func TestIndexDeleteHeavyCompaction(t *testing.T) {
	idx := newIndex()
	const n = 5000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("dk-%d", i))
		idx.put(keys[i], location{1, uint32(i), 1})
	}
	// Delete 90% of keys, forcing tombstone-heavy occupancy and exercising
	// the same-size rehash-to-compact path (not just grow-on-load).
	for i := 0; i < n; i++ {
		if i%10 != 0 {
			idx.delete(keys[i])
		}
	}
	if idx.len() != n/10 {
		t.Fatalf("expected %d survivors, got %d", n/10, idx.len())
	}
	// Insert enough new keys to force rehashing through the tombstone-heavy
	// table and confirm nothing is lost or misplaced.
	for i := 0; i < n; i++ {
		idx.put([]byte(fmt.Sprintf("new-%d", i)), location{2, uint32(i), 2})
	}
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			if _, found := idx.get(keys[i]); !found {
				t.Fatalf("survivor key %d lost after compaction/rehash", i)
			}
		}
	}
	for i := 0; i < n; i++ {
		if _, found := idx.get([]byte(fmt.Sprintf("new-%d", i))); !found {
			t.Fatalf("new key %d missing", i)
		}
	}
}

func TestArenaChunkBoundary(t *testing.T) {
	a := newArena(64) // tiny chunk size to force many boundary crossings
	type loc struct {
		chunk uint16
		off   uint32
		key   string
	}
	var locs []loc
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("arena-key-%03d", i)
		c, o := a.put([]byte(k))
		locs = append(locs, loc{c, o, k})
	}
	for _, l := range locs {
		got := a.get(l.chunk, l.off, uint16(len(l.key)))
		if string(got) != l.key {
			t.Fatalf("arena roundtrip mismatch: want %q got %q", l.key, got)
		}
	}
}

func TestArenaOversizedKey(t *testing.T) {
	a := newArena(16)
	big := make([]byte, 1000)
	for i := range big {
		big[i] = byte(i)
	}
	c, o := a.put(big)
	got := a.get(c, o, uint16(len(big)))
	if string(got) != string(big) {
		t.Fatal("oversized key roundtrip failed")
	}
	// A normal small key inserted after must not corrupt or be corrupted by
	// the oversized chunk.
	c2, o2 := a.put([]byte("small"))
	got2 := a.get(c2, o2, 5)
	if string(got2) != "small" {
		t.Fatalf("post-oversized insert corrupted: got %q", got2)
	}
}

func TestIndexRandomizedFuzz(t *testing.T) {
	idx := newIndex()
	model := map[string]location{}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("fk-%d", r.Intn(3000))
		switch r.Intn(3) {
		case 0, 1: // put weighted higher than delete
			loc := location{uint32(r.Intn(100)), uint32(r.Intn(1 << 20)), uint32(r.Intn(1000))}
			idx.put([]byte(key), loc)
			model[key] = loc
		case 2:
			idx.delete([]byte(key))
			delete(model, key)
		}
	}
	if idx.len() != len(model) {
		t.Fatalf("count mismatch: index=%d model=%d", idx.len(), len(model))
	}
	for key, want := range model {
		loc, found := idx.get([]byte(key))
		if !found {
			t.Fatalf("key %q missing", key)
		}
		if loc != want {
			t.Fatalf("key %q mismatch: want %+v got %+v", key, want, loc)
		}
	}
}
