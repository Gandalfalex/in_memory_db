package kv

import (
	"fmt"
	"runtime"
	"testing"
)

// All benchmarks pin GOMAXPROCS(1): the design target is a single-CPU
// host, so results on the dev machine's full core count would be
// misleading about the workload this package is built for.

func openBenchDB(b *testing.B) *DB {
	b.Helper()
	prev := runtime.GOMAXPROCS(1)
	b.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	db, err := Open(DefaultOptions(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func BenchmarkPut(b *testing.B) {
	db := openBenchDB(b)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%d", i), val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db := openBenchDB(b)
	val := make([]byte, 100)
	const preload = 100000
	keys := make([][]byte, preload)
	for i := range preload {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
		if err := db.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%preload]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPrefixScan(b *testing.B) {
	db := openBenchDB(b)
	val := make([]byte, 100)
	const preload = 20000
	for i := range preload {
		if err := db.Put(fmt.Appendf(nil, "scan-%06d", i), val); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := db.Iterator(IterOptions{Prefix: []byte("scan-")})
		count := 0
		for it.Next() {
			count++
		}
		it.Close()
		if count != preload {
			b.Fatalf("expected %d entries, got %d", preload, count)
		}
	}
}
