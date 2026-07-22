package kv

import "testing"

// FuzzRecordRoundtrip checks the core property of the on-disk record
// format: whatever encodeRecord writes, verifyChecksum must accept and
// decodeHeader must decode back to the original lengths/timestamp/flag —
// for arbitrary key/value bytes, not just the handful of cases the
// table-driven db_test.go tests cover.
func FuzzRecordRoundtrip(f *testing.F) {
	f.Add([]byte("key"), []byte("value"), false, int64(1700000000))
	f.Add([]byte(""), []byte(""), true, int64(0))
	f.Add([]byte{0, 1, 2}, []byte{0xFF, 0xFE}, false, int64(-1))

	f.Fuzz(func(t *testing.T, key, value []byte, tombstone bool, timestamp int64) {
		if len(key) > 1<<16 || len(value) > 1<<20 {
			t.Skip() // not the interesting case here; keeps iterations fast
		}
		buf := encodeRecord(key, value, tombstone, timestamp)

		if !verifyChecksum(buf) {
			t.Fatalf("freshly encoded record failed its own checksum")
		}
		h := decodeHeader(buf)
		if h.keyLen != uint32(len(key)) {
			t.Fatalf("decoded keyLen = %d, want %d", h.keyLen, len(key))
		}
		if h.valLen != uint32(len(value)) {
			t.Fatalf("decoded valLen = %d, want %d", h.valLen, len(value))
		}
		if h.timestamp != timestamp {
			t.Fatalf("decoded timestamp = %d, want %d", h.timestamp, timestamp)
		}
		if h.tombstone() != tombstone {
			t.Fatalf("decoded tombstone = %v, want %v", h.tombstone(), tombstone)
		}

		// Flipping any one byte must not still pass the checksum — the
		// whole point of storing one. Try a handful of positions rather
		// than every byte, to keep this fast at fuzz scale.
		for _, pos := range []int{0, len(buf) / 2, len(buf) - 1} {
			if pos < 0 || pos >= len(buf) {
				continue
			}
			mutated := append([]byte(nil), buf...)
			mutated[pos] ^= 0xFF
			if verifyChecksum(mutated) {
				t.Fatalf("flipping byte %d still passed checksum", pos)
			}
		}
	})
}

// FuzzVerifyChecksumNeverPanics targets the actual trust boundary:
// verifyChecksum runs against bytes read back off disk, which recovery
// treats as untrusted (a torn write, truncation, or corruption) — it
// must handle any length or content without panicking, never just
// "assume the caller passed a well-formed record."
func FuzzVerifyChecksumNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3})
	f.Add(make([]byte, headerSize-1))
	f.Add(make([]byte, headerSize))
	f.Add(encodeRecord([]byte("k"), []byte("v"), false, 123))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = verifyChecksum(data)
	})
}
