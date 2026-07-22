package bitcask

import "testing"

func TestPagesResidentBounds(t *testing.T) {
	data := make([]byte, 100)
	if pagesResident(data, 0, 0) {
		t.Fatal("zero-length range should report not-resident (nothing to confirm)")
	}
	if pagesResident(data, -1, 10) {
		t.Fatal("negative offset should be rejected")
	}
	if pagesResident(data, 50, 100) {
		t.Fatal("out-of-range span should be rejected")
	}
}

// TestPagesResidentJustWrittenIsResident is a smoke test: a page this
// process just wrote to (via a real segment, so the address is
// mmap-backed) must report resident — otherwise every Get() would
// pointlessly take the pread fallback path on unix.
func TestPagesResidentJustWrittenIsResident(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hot"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	db.mu.RLock()
	loc, found := db.idx.get([]byte("hot"))
	if !found {
		db.mu.RUnlock()
		t.Fatal("key not found in index")
	}
	seg := db.segments[loc.segID]
	data := seg.mf.Bytes()
	db.mu.RUnlock()

	if !pagesResident(data, int(loc.valOffset), int(loc.valLen)) {
		t.Fatal("just-written page should be resident")
	}
}
