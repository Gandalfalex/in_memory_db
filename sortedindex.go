package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iter"
	"os"
	"sort"
	"sync"
)

// On-disk format constants shared between the build path
// (sortedindex_build.go) and the read path below.
const (
	sidxMagic   = "KVSI"
	sparseMagic = "KVSP"
	sidxVersion = 1
)

func sparsePathFor(sidxPath string) string { return sidxPath + ".sparse" }

// SortedIndex is a read-only, lexicographically-sorted directory over one
// or more FileIndex-style source files (see fileindex.go), applied in
// caller-given precedence order — typically a one-shot base file followed
// by incremental change files, later file wins on a key conflict, same
// "last write wins" rule as within a single file. Unlike FileIndex, whose
// map[string]lineLoc index is fully RAM-resident, SortedIndex keeps the
// full key set on disk and only a sparse sample of it in RAM, so its
// memory footprint is independent of key count — built for source data
// too large to index in RAM (tens of millions of lines and up).
//
// It is built once by BuildSortedIndex (an external merge sort: the
// combined source data can be far larger than RAM, see
// sortedindex_build.go) and reopened read-only by OpenSortedIndex, which
// is self-describing — it reads the source file list back from the
// .sources sidecar, so callers don't repeat it. There is no Put; a
// SortedIndex is a point-in-time view over its sources at build time.
// EnsureFresh (sortedindex_sources.go) is the usual entry point: it
// rebuilds only when the sources have actually changed (a stat-only
// check), otherwise just reopens the existing cache — see its doc
// comment, and SortedIndexManager for pairing that with idle-TTL reaping
// so a rarely queried dataset costs nothing in RAM between bursts of use.
//
// On-disk layout (all part of one build, sharing sidxPath as a base name):
//
//	<sidxPath> (sorted key directory, one entry per source line, ascending key order):
//	  magic "KVSI"(4) | version(4) | count(8)
//	  count * [ keyLen(2) | key | fileIdx(2) | srcOffset(8) | lineLen(4) ]
//	  crc32(4) -- IEEE checksum over every entry byte (not the header)
//
//	<sidxPath>.sparse (RAM-loaded directory, every SparseInterval-th key):
//	  magic "KVSP"(4) | version(4) | count(8)
//	  count * [ keyLen(2) | key | sidxOffset(8) ]  -- sidxOffset points at
//	    that entry's start within <sidxPath>, header included
//	  crc32(4)
//
//	<sidxPath>.bloom (RAM-loaded Get-gating filter; see bloom.go)
//
//	<sidxPath>.sources (RAM-loaded freshness record; see
//	  sortedindex_sources.go): the ordered source paths plus the
//	  size/mtime each had at build time, letting EnsureFresh detect
//	  "sources changed" with a stat, not a rebuild.
//
// Get binary-searches the in-RAM sparse directory for the last sampled
// key <= the target, then does one bounded sequential scan of <sidxPath>
// from that offset (at most SparseInterval entries) to find the exact
// entry, then a single pread of the owning source file for the line
// itself. All files are pread-based (os.File.ReadAt), not mmap'd: unlike
// the segment reads in segment.go, these are one-shot random reads
// scattered across a huge file, not hot repeatedly-touched ranges, so
// there is little to gain from mmap+mincore here and pread keeps the Go
// scheduler able to park the goroutine during the I/O (see
// mincore_unix.go's doc comment for why that matters on a single-CPU
// host).
type SortedIndex struct {
	sourcePaths []string
	sidxPath    string
	keyFunc     KeyFunc

	srcs []*os.File
	sidx *os.File

	entriesStart int64 // fixed sidx header size
	entriesEnd   int64 // sidx file size minus trailing crc32
	count        int64

	sparse []sparseEntry // full in-RAM sparse directory; small by construction
	bloom  *bloomFilter  // nil if built with BloomFPR < 0, or opened from an index built before this existed

	// mu guards closed and every fd read (si.sidx, si.srcs), scoped tightly
	// per read rather than held across a whole All()/Prefix() scan: Get
	// takes it for one bounded call (mirrors FileIndex.Get), Prefix/All
	// take and release it once per yielded entry (mirrors FileIndex's
	// Iterator, which is a fi.Get call per item) — so Close never blocks
	// on an in-progress large scan, only on the one entry currently being
	// read, and any read that loses the race with Close observes ErrClosed
	// instead of a raw "file already closed" error from a closed fd.
	mu     sync.RWMutex
	closed bool
}

// sparseEntry is one sample in the in-RAM sparse directory.
type sparseEntry struct {
	key    []byte
	offset int64 // byte offset of this key's entry within <sidxPath>
}

// sidxEntry is one decoded entry read back from a sidx file.
type sidxEntry struct {
	key     []byte
	fileIdx uint16
	srcOff  int64
	length  int32
}

// SortedIndexOptions tunes the RAM/lookup-cost tradeoff of a build.
type SortedIndexOptions struct {
	// ChunkEntries bounds how many (key, location) entries are held in RAM
	// at once while scanning the source files, before that chunk is sorted
	// and spilled to a temporary run file. Defaults to 2,000,000, which
	// keeps a single chunk's RAM in the tens-of-MB range for typical key
	// sizes while still bounding the number of runs (and therefore the
	// merge's open-file-descriptor count) for a 78M-line source to ~40.
	ChunkEntries int
	// SparseInterval is how many sorted entries separate consecutive
	// samples kept in the RAM sparse directory. Larger = less RAM, more
	// bytes scanned per Get. Defaults to 4096, which for a 78M-key
	// source keeps the sparse directory around ~19k entries (a few
	// hundred KB to a few MB depending on key size) while bounding every
	// Get's sequential scan to at most 4096 entries (~tens of KB read).
	SparseInterval int
	// BloomFPR is the target false-positive rate for the Get-gating
	// Bloom filter built alongside the index. Defaults to 0.01 (1%), which
	// costs ~9.6 bits/key regardless of key length — for 78M keys, ~94MB
	// flat, fully RAM-resident like the sparse directory but far cheaper
	// per key when keys are long. Set to a negative value to skip building
	// a Bloom filter entirely.
	BloomFPR float64
}

func (o SortedIndexOptions) withDefaults() SortedIndexOptions {
	if o.ChunkEntries <= 0 {
		o.ChunkEntries = 2_000_000
	}
	if o.SparseInterval <= 0 {
		o.SparseInterval = 4096
	}
	if o.BloomFPR == 0 {
		o.BloomFPR = 0.01
	}
	return o
}

// --- open / read ---------------------------------------------------------

// OpenSortedIndex opens a directory previously written by BuildSortedIndex.
// It is self-describing: the source file list comes from the .sources
// sidecar (see sortedindex_sources.go), not a caller argument. It loads
// the (small, RAM-bounded) sparse and Bloom sidecars fully into memory and
// keeps every source file and the sidx file open for pread-based lookups.
// It does not load the full sidx or any source file into RAM, but it does
// do one sequential read over the sidx file's entries region to verify
// its checksum (see verifySidxChecksum) before trusting it — an O(n) cost
// on every Open, deliberately: see that function's doc comment for why.
func OpenSortedIndex(sidxPath string, keyFunc KeyFunc) (*SortedIndex, error) {
	stats, err := readSourcesFile(sourcesPathFor(sidxPath))
	if err != nil {
		return nil, err
	}
	srcPaths := make([]string, len(stats))
	srcs := make([]*os.File, len(stats))
	for i, s := range stats {
		f, err := os.Open(s.path)
		if err != nil {
			closeAll(srcs[:i])
			return nil, fmt.Errorf("kv: open source %q: %w", s.path, err)
		}
		srcPaths[i] = s.path
		srcs[i] = f
	}

	sidx, err := os.Open(sidxPath)
	if err != nil {
		closeAll(srcs)
		return nil, fmt.Errorf("kv: open sidx: %w", err)
	}

	info, err := sidx.Stat()
	if err != nil {
		closeAll(srcs)
		sidx.Close()
		return nil, err
	}

	count, err := verifySidxHeader(sidx)
	if err != nil {
		closeAll(srcs)
		sidx.Close()
		return nil, err
	}

	if err := verifySidxChecksum(sidx, info.Size()); err != nil {
		closeAll(srcs)
		sidx.Close()
		return nil, err
	}

	sparse, err := readSparseFile(sparsePathFor(sidxPath))
	if err != nil {
		closeAll(srcs)
		sidx.Close()
		return nil, err
	}

	bloom, err := readBloomFile(bloomPathFor(sidxPath)) // nil, nil if the sidecar doesn't exist
	if err != nil {
		closeAll(srcs)
		sidx.Close()
		return nil, err
	}

	return &SortedIndex{
		sourcePaths:  srcPaths,
		sidxPath:     sidxPath,
		keyFunc:      keyFunc,
		srcs:         srcs,
		sidx:         sidx,
		entriesStart: int64(len(sidxMagic) + 4 + 8),
		entriesEnd:   info.Size() - 4, // exclude trailing crc32
		count:        count,
		sparse:       sparse,
		bloom:        bloom,
	}, nil
}

func closeAll(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}

func verifySidxHeader(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReader(f)
	magic := make([]byte, len(sidxMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != sidxMagic {
		return 0, fmt.Errorf("kv: sidx: bad magic")
	}
	version, err := readUint32(r)
	if err != nil || version != sidxVersion {
		return 0, fmt.Errorf("kv: sidx: unsupported version")
	}
	count, err := readUint64(r)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

// verifySidxChecksum does a streaming pass over the sidx file's entries
// region (everything after the fixed header, before the trailing crc32 —
// per the format's "crc32 over every entry byte, not the header") to
// confirm it matches before any entry is trusted. Call only after
// verifySidxHeader has confirmed the header's basic shape.
//
// This is an O(n) sequential read of the whole file on every Open — the
// same tradeoff index_snapshot.go's verifySnapshotChecksum already makes
// for the DB's checkpoint file: paid once per (re)open, not per Get or
// Prefix call, and safety against silent corruption (bit rot, a torn
// write past the fixed header) outweighs the cost, which even at
// multi-GB scale is a fast sequential read relative to what a rebuild
// would cost.
func verifySidxChecksum(f *os.File, size int64) error {
	const headerLen = int64(len(sidxMagic) + 4 + 8) // magic + version + count
	if size < headerLen+4 {
		return fmt.Errorf("kv: sidx: truncated")
	}
	if _, err := f.Seek(headerLen, io.SeekStart); err != nil {
		return err
	}
	crc := crc32.NewIEEE()
	if _, err := io.CopyN(crc, f, size-headerLen-4); err != nil {
		return fmt.Errorf("kv: sidx: %w", err)
	}
	var stored [4]byte
	if _, err := io.ReadFull(f, stored[:]); err != nil {
		return fmt.Errorf("kv: sidx: %w", err)
	}
	if binary.BigEndian.Uint32(stored[:]) != crc.Sum32() {
		return fmt.Errorf("kv: sidx: checksum mismatch")
	}
	return nil
}

func readSparseFile(path string) ([]sparseEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("kv: open sparse: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 4 {
		return nil, fmt.Errorf("kv: sparse: truncated")
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
		return nil, fmt.Errorf("kv: sparse: checksum mismatch")
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	magic := make([]byte, len(sparseMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != sparseMagic {
		return nil, fmt.Errorf("kv: sparse: bad magic")
	}
	if _, err := readUint32(r); err != nil {
		return nil, err
	}
	count, err := readUint64(r)
	if err != nil {
		return nil, err
	}
	out := make([]sparseEntry, 0, count)
	for range count {
		keyLen, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}
		off, err := readUint64(r)
		if err != nil {
			return nil, err
		}
		out = append(out, sparseEntry{key: key, offset: int64(off)})
	}
	return out, nil
}

// readSidxEntry reads one entry from a stream positioned at an entry
// boundary within a sidx file's entries region.
func readSidxEntry(r *bufio.Reader) (sidxEntry, error) {
	keyLen, err := readUint16(r)
	if err != nil {
		return sidxEntry{}, err // io.EOF at a clean entry boundary
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return sidxEntry{}, fmt.Errorf("kv: truncated sidx: %w", err)
	}
	fileIdx, err := readUint16(r)
	if err != nil {
		return sidxEntry{}, err
	}
	srcOff, err := readUint64(r)
	if err != nil {
		return sidxEntry{}, err
	}
	length, err := readUint32(r)
	if err != nil {
		return sidxEntry{}, err
	}
	return sidxEntry{key: key, fileIdx: fileIdx, srcOff: int64(srcOff), length: int32(length)}, nil
}

// Close closes every underlying source and the sidx file descriptor. A
// second call, or any Get/Prefix/All call after Close, returns ErrClosed.
func (si *SortedIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.closed {
		return ErrClosed
	}
	si.closed = true
	var errs []error
	for _, f := range si.srcs {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := si.sidx.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Len returns the number of keys in the index.
func (si *SortedIndex) Len() int64 { return si.count }

// SourcePaths returns the ordered source file list this index was built
// from (lowest precedence first), as recorded in the .sources sidecar.
func (si *SortedIndex) SourcePaths() []string { return si.sourcePaths }

// floorOffset returns the sidx byte offset to start a sequential scan
// from in order to find target: the offset of the last sparse-sampled key
// <= target, or entriesStart if target sorts before every sampled key.
func (si *SortedIndex) floorOffset(target []byte) int64 {
	n := len(si.sparse)
	i := sort.Search(n, func(i int) bool { return bytes.Compare(si.sparse[i].key, target) > 0 })
	if i == 0 {
		return si.entriesStart
	}
	return si.sparse[i-1].offset
}

// Get returns the current line for key, or ErrNotFound — same contract as
// DB.Get and FileIndex.Get. If a Bloom filter sidecar was built, a
// definite-absent answer from it short-circuits here with zero I/O;
// otherwise (and on any maybe-present answer) it falls through to reading
// at most one bounded (<= SparseInterval entries) sequential scan of the
// sidx file plus one pread of the owning source file — RAM cost is O(1)
// regardless of index size either way.
func (si *SortedIndex) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	si.mu.RLock()
	defer si.mu.RUnlock()
	if si.closed {
		return nil, ErrClosed
	}
	if si.bloom != nil && !si.bloom.mayContain(key) {
		return nil, ErrNotFound
	}
	start := si.floorOffset(key)
	sr := io.NewSectionReader(si.sidx, start, si.entriesEnd-start)
	r := bufio.NewReader(sr)
	for {
		e, err := readSidxEntry(r)
		if errors.Is(err, io.EOF) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		cmp := bytes.Compare(e.key, key)
		if cmp == 0 {
			line := make([]byte, e.length)
			if _, err := si.srcs[e.fileIdx].ReadAt(line, e.srcOff); err != nil {
				return nil, fmt.Errorf("kv: read source line: %w", err)
			}
			return line, nil
		}
		if cmp > 0 {
			return nil, ErrNotFound // sorted order: key is absent
		}
	}
}

// Has reports whether key currently has a live entry — same contract as
// DB.Has and FileIndex.Has. Implemented as Get and discarding the line,
// since Get is already the cheapest possible existence check here (Bloom
// gate, then a bounded scan).
func (si *SortedIndex) Has(key []byte) (bool, error) {
	_, err := si.Get(key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Prefix returns an iterator over (key, line) pairs whose key has the
// given prefix, in ascending key order, doing one bounded seek to the
// start of the range followed by a purely sequential scan — no full-index
// scan, no RAM proportional to result size or index size.
func (si *SortedIndex) Prefix(prefix []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func(key, line []byte) bool) {
		start := si.floorOffset(prefix)
		sr := io.NewSectionReader(si.sidx, start, si.entriesEnd-start)
		r := bufio.NewReader(sr)
		for {
			key, line, ok := si.nextPrefixMatch(r, prefix)
			if !ok {
				return
			}
			if !yield(key, line) {
				return
			}
		}
	}
}

// nextPrefixMatch reads forward from r for the next entry matching
// prefix, taking si.mu for just this one entry's read (and any entries
// skipped before it, still within floorOffset's small margin) rather than
// for the whole scan — see the mu doc comment on SortedIndex. ok is false
// at end of range, end of file, Close having happened, or a read error;
// callers other than Prefix don't need to distinguish those.
func (si *SortedIndex) nextPrefixMatch(r *bufio.Reader, prefix []byte) (key, line []byte, ok bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if si.closed {
		return nil, nil, false
	}
	for {
		e, err := readSidxEntry(r)
		if err != nil {
			return nil, nil, false // io.EOF or a read error both just end iteration
		}
		if bytes.Compare(e.key, prefix) < 0 {
			continue // floorOffset may land slightly before the range
		}
		if !bytes.HasPrefix(e.key, prefix) {
			return nil, nil, false // sorted order: nothing after this can match either
		}
		lineBuf := make([]byte, e.length)
		if _, err := si.srcs[e.fileIdx].ReadAt(lineBuf, e.srcOff); err != nil {
			return nil, nil, false
		}
		return e.key, lineBuf, true
	}
}

// All returns an iterator over every (key, line) pair in ascending key
// order: a single sequential scan of the sidx file interleaved with
// preads of the source files, with no RAM cost beyond one buffered reader
// regardless of index size.
func (si *SortedIndex) All() iter.Seq2[[]byte, []byte] {
	return si.Prefix(nil)
}

// FilterAll wraps an (key, line) iterator (typically SortedIndex.All or
// Prefix) with a caller predicate over the raw line bytes — a generic
// combinator, kept here rather than pushed into app code so every caller
// gets streaming, RAM-independent filtering for free. Field-specific
// predicate logic (which JSON fields to check, what values to match)
// stays the app's concern; only the plumbing lives in the library.
func FilterAll(seq iter.Seq2[[]byte, []byte], keep func(line []byte) bool) iter.Seq2[[]byte, []byte] {
	return func(yield func(key, line []byte) bool) {
		seq(func(key, line []byte) bool {
			if !keep(line) {
				return true
			}
			return yield(key, line)
		})
	}
}
