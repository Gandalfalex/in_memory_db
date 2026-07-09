//go:build scale

package kv

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// TestMemoryBoundAtScale is the direct evidence for this package's central
// claim: a few million records stay within a bounded heap footprint on a
// single core, because the index (index.go) is a byte-arena-backed table
// rather than map[string]indexEntry. It's gated behind the "scale" build
// tag (run explicitly via `go test -tags scale -run TestMemoryBoundAtScale
// -timeout 20m`) since it inserts millions of records and is too slow for
// a normal `go test ./...` run.
func TestMemoryBoundAtScale(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 4_000_000
	const boundBytes = 500 << 20 // 500MB heap ceiling, well inside a 1GB host budget

	val := make([]byte, 100)
	key := make([]byte, 20)
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if i > 0 && i%500_000 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			t.Logf("i=%d HeapAlloc=%dMB NumGC=%d", i, m.HeapAlloc/(1<<20), m.NumGC)
		}
	}

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("final: n=%d HeapAlloc=%dMB HeapSys=%dMB NumGC=%d PauseTotal=%dms",
		n, m.HeapAlloc/(1<<20), m.HeapSys/(1<<20), m.NumGC, m.PauseTotalNs/1e6)

	if m.HeapAlloc > boundBytes {
		t.Fatalf("HeapAlloc %dMB exceeds %dMB bound at %d records", m.HeapAlloc/(1<<20), boundBytes/(1<<20), n)
	}

	// Spot-check correctness wasn't sacrificed for the memory result.
	for _, i := range []int{0, n / 2, n - 1} {
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get %d after scale insert: %v", i, err)
		}
	}
}
