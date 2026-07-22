package sortedindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"io"
	"math"
	"os"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

const bloomMagic = "KVBF"

// bloomPathFor is the sidecar path a SortedIndex's bloom filter is
// written to/read from, alongside its .sidx and .sparse siblings.
func bloomPathFor(sidxPath string) string { return sidxPath + ".bloom" }

// bloomFilter is a standard bit-array Bloom filter gating SortedIndex.Get:
// a "definitely absent" answer skips the sidx scan and source pread
// entirely, so negative lookups cost zero I/O. It never gates positive
// lookups — false positives just fall through to the normal sparse-scan
// path, so correctness never depends on the filter.
//
// Its two hash functions must be reproducible across process runs (built
// once, queried by later, possibly different, processes), which rules out
// hash/maphash (seeded randomly per-process by design, for DoS
// resistance). fnv and crc64 are both deterministic stdlib hashes with no
// such caveat; k hash functions are then derived from the two via the
// standard Kirsch-Mitzenmacher combination (g_i(x) = h1 + i*h2 mod m)
// instead of needing k independent hashers.
type bloomFilter struct {
	bits []byte
	m    uint64 // number of bits
	k    uint64 // number of hash probes per key
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

// newBloomFilter sizes a filter for n keys at false-positive rate p using
// the standard optimal-m/optimal-k formulas.
func newBloomFilter(n int64, p float64) *bloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	m := max(uint64(math.Ceil(-1*float64(n)*math.Log(p)/(math.Ln2*math.Ln2))), 8)
	k := max(uint64(math.Round(float64(m)/float64(n)*math.Ln2)), 1)
	return &bloomFilter{bits: make([]byte, (m+7)/8), m: m, k: k}
}

// bloomHashes computes the two independent base hashes key's k probe
// positions are derived from.
func bloomHashes(key []byte) (h1, h2 uint64) {
	f := fnv.New64a()
	f.Write(key)
	h1 = f.Sum64()
	h2 = crc64.Checksum(key, crc64Table)
	if h2 == 0 {
		h2 = 1 // avoid a degenerate all-zero step that would probe one bit k times
	}
	return h1, h2
}

func (b *bloomFilter) add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < b.k; i++ {
		bit := (h1 + i*h2) % b.m
		b.bits[bit/8] |= 1 << (bit % 8)
	}
}

// mayContain reports whether key might be present (true) or is
// definitely absent (false). It never false-negatives.
func (b *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < b.k; i++ {
		bit := (h1 + i*h2) % b.m
		if b.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// writeBloomFile persists b to path in one shot: magic, version, m, k,
// the bit array, then a trailing crc32 — small (m/8 bytes: ~94MB for 78M
// keys at 1% FPR) so no chunking/streaming is needed.
func writeBloomFile(path string, b *bloomFilter) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriter(f)
	crc := crc32.NewIEEE()
	w := io.MultiWriter(bw, crc)
	if _, err := io.WriteString(w, bloomMagic); err != nil {
		return err
	}
	if err := kvtypes.WriteUint32(w, sidxVersion); err != nil {
		return err
	}
	if err := kvtypes.WriteUint64(w, b.m); err != nil {
		return err
	}
	if err := kvtypes.WriteUint64(w, b.k); err != nil {
		return err
	}
	if _, err := w.Write(b.bits); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.BigEndian, crc.Sum32()); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	ok = true
	return os.Rename(tmpPath, path)
}

// readBloomFile loads a bloom filter fully into RAM (by construction,
// small: fixed bits/key regardless of key length/count scale). Returns
// (nil, nil) if path doesn't exist, so callers built by an older
// BuildSortedIndex or with bloom filtering skipped degrade gracefully
// (Lookup just doesn't get the negative-lookup fast path, nothing else
// changes).
func readBloomFile(path string) (*bloomFilter, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("kv: open bloom: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 4 {
		return nil, fmt.Errorf("kv: bloom: truncated")
	}
	crc := crc32.NewIEEE()
	if _, err := io.CopyN(crc, f, info.Size()-4); err != nil {
		return nil, err
	}
	var stored [4]byte
	if _, err := io.ReadFull(f, stored[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(stored[:]) != crc.Sum32() {
		return nil, fmt.Errorf("kv: bloom: checksum mismatch")
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	magic := make([]byte, len(bloomMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != bloomMagic {
		return nil, fmt.Errorf("kv: bloom: bad magic")
	}
	if _, err := kvtypes.ReadUint32(r); err != nil {
		return nil, err
	}
	m, err := kvtypes.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	k, err := kvtypes.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	bits := make([]byte, (m+7)/8)
	if _, err := io.ReadFull(r, bits); err != nil {
		return nil, fmt.Errorf("kv: bloom: truncated bits: %w", err)
	}
	return &bloomFilter{bits: bits, m: m, k: k}, nil
}
