package kv

import "testing"

// testOptions mirrors internal/bitcask's own test helper of the same
// name: kept here too since api_test.go, kv_test.go, and bench_test.go
// are black-box (only exercise the public façade) and stay at the module
// root rather than moving into internal/bitcask with the white-box tests.
func testOptions(t *testing.T) Options {
	t.Helper()
	opts := DefaultOptions(t.TempDir())
	opts.SegmentSize = 4096 // small, to exercise rotation quickly in tests
	return opts
}
