package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Gandalfalex/in_memory_db/internal/mmapfile"
)

// cachePageSizeBytes is used to size/index each segment's residency
// bitmap (see residentBits below). It doesn't need to be the exact OS
// page size mincore(2) operates on — pagesResident does its own
// page-aligned rounding against the real page size internally — this
// value only needs to divide the address space into cache buckets no
// coarser than that, so os.Getpagesize() (same on every platform this
// process runs on) is the natural choice.
var cachePageSizeBytes = int64(os.Getpagesize())

// segment is one append-only, memory-mapped data file. The active segment
// is preallocated to capacity bytes and mapped once so appends are plain
// slice writes with no remapping; older, rotated-out segments are
// finalize()'d, shrinking the mapping down to their actual used size.
type segment struct {
	id       uint32
	path     string
	mf       *mmapfile.File
	capacity int64
	size     int64 // bytes currently used (written) within the mapped region

	// residentBits caches confirmed-resident pages (one bit per OS page),
	// so repeated reads of hot data pay mincore's syscall cost once, not
	// on every Get. See cachedPagesResident and pagesResident's doc
	// comment (mincore_unix.go) for why this exists at all. Periodically
	// cleared (residentCleanupInterval) since a bit set here only means
	// "confirmed resident as of that check" — the OS remains free to evict
	// the page later under memory pressure.
	residentBits    []atomic.Uint64
	residentBitsSet sync.Once
	nextCleanup     atomic.Int64
}

const residentCleanupInterval = 60 * time.Second

func (s *segment) ensureResidentBits() {
	s.residentBitsSet.Do(func() {
		pages := (s.capacity + cachePageSizeBytes - 1) / cachePageSizeBytes
		words := (pages + 63) / 64
		if words < 1 {
			words = 1
		}
		s.residentBits = make([]atomic.Uint64, words)
		s.nextCleanup.Store(time.Now().Add(residentCleanupInterval).Unix())
	})
}

// markResident records that [offset, offset+length) is known resident, with
// no syscall needed: called right after append(), where the caller (this
// same process) just wrote those exact bytes into the mapping, so their
// residency isn't in question.
func (s *segment) markResident(offset, length int64) {
	s.ensureResidentBits()
	start := offset - offset%cachePageSizeBytes
	end := offset + length
	for off := start; off < end; off += cachePageSizeBytes {
		pageIdx := off / cachePageSizeBytes
		wordIdx := pageIdx / 64
		mask := uint64(1) << uint(pageIdx%64)
		for {
			word := s.residentBits[wordIdx].Load()
			if word&mask != 0 || s.residentBits[wordIdx].CompareAndSwap(word, word|mask) {
				break
			}
		}
	}
}

// cachedPagesResident is pagesResident with a per-segment cache in front of
// it: pages already confirmed resident are trusted without a repeat
// mincore call. The cache is cleared periodically (not trusted forever),
// since the OS can evict a file-backed page any time after it was checked.
func (s *segment) cachedPagesResident(data []byte, offset, length int) bool {
	if length <= 0 || offset < 0 || int64(offset+length) > int64(len(data)) {
		return false
	}
	s.ensureResidentBits()

	now := time.Now().Unix()
	if next := s.nextCleanup.Load(); now > next && s.nextCleanup.CompareAndSwap(next, now+int64(residentCleanupInterval.Seconds())) {
		for i := range s.residentBits {
			s.residentBits[i].Store(0)
		}
	}

	start := int64(offset) - int64(offset)%cachePageSizeBytes
	end := int64(offset + length)
	for off := start; off < end; off += cachePageSizeBytes {
		pageIdx := off / cachePageSizeBytes
		wordIdx := pageIdx / 64
		mask := uint64(1) << uint(pageIdx%64)
		if s.residentBits[wordIdx].Load()&mask != 0 {
			continue // already confirmed resident
		}
		if !pagesResident(data, int(off), 1) {
			return false
		}
		for {
			word := s.residentBits[wordIdx].Load()
			if word&mask != 0 || s.residentBits[wordIdx].CompareAndSwap(word, word|mask) {
				break
			}
		}
	}
	return true
}

func segmentPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.seg", id))
}

// createSegment makes a brand new, empty segment file preallocated to
// capacity bytes.
func createSegment(dir string, id uint32, capacity int64) (*segment, error) {
	path := segmentPath(dir, id)
	mf, err := mmapfile.Create(path, capacity)
	if err != nil {
		return nil, fmt.Errorf("create segment %d: %w", id, err)
	}
	return &segment{id: id, path: path, mf: mf, capacity: capacity, size: 0}, nil
}

// openSegment maps an existing segment file for reading (and, if it's the
// active segment, further appends). The caller doesn't need to know the
// segment's used size up front: pass 0 and use forEach to scan and
// determine it (see recovery.go), since unwritten preallocated space is
// self-describing (see record.go's CRC comment).
func openSegment(dir string, id uint32) (*segment, error) {
	path := segmentPath(dir, id)
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", id, err)
	}
	mf, err := mmapfile.Open(path, info.Size())
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", id, err)
	}
	return &segment{id: id, path: path, mf: mf, capacity: info.Size(), size: 0}, nil
}

// remaining reports how many bytes are free before the segment hits its
// preallocated capacity.
func (s *segment) remaining() int64 {
	return s.capacity - s.size
}

// append writes record to the tail of the segment and returns the offset
// it was written at. The caller must have already checked remaining() >=
// len(record) (db.go decides segment rotation, not segment itself).
func (s *segment) append(record []byte) (int64, error) {
	dst := s.mf.Bytes()
	off := s.size
	if off+int64(len(record)) > int64(len(dst)) {
		return 0, fmt.Errorf("segment %d: record of %d bytes does not fit at offset %d (capacity %d)", s.id, len(record), off, len(dst))
	}
	copy(dst[off:], record)
	s.size = off + int64(len(record))
	s.markResident(off, int64(len(record)))
	return off, nil
}

// readAt returns the length bytes at offset within the segment. When the
// range is confirmed resident in RAM, this is a zero-copy slice aliasing
// mmap'd memory (must not be retained past the segment's lifetime, i.e.
// past compaction dropping it). When residency can't be confirmed, it
// falls back to a real positioned read (pread) into a freshly allocated
// buffer instead of touching the mmap directly — see pagesResident's doc
// comment (mincore_unix.go) for why that matters on a single-CPU host.
func (s *segment) readAt(offset, length int64) ([]byte, error) {
	data := s.mf.Bytes()
	if offset < 0 || length < 0 || offset+length > int64(len(data)) {
		return nil, fmt.Errorf("segment %d: read out of range (offset=%d length=%d mapped=%d)", s.id, offset, length, len(data))
	}
	if s.cachedPagesResident(data, int(offset), int(length)) {
		return data[offset : offset+length], nil
	}
	buf := make([]byte, length)
	n, err := s.mf.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("segment %d: fallback read at offset %d: %w", s.id, offset, err)
	}
	if int64(n) != length {
		return nil, fmt.Errorf("segment %d: short fallback read at offset %d: got %d want %d", s.id, offset, n, length)
	}
	return buf, nil
}

// forEach sequentially walks well-formed records starting at fromOffset,
// calling fn for each. It stops (without returning an error) at the first
// point a full, checksum-valid record can no longer be read — this is the
// normal way to find "the end of the log", whether that's genuine EOF,
// unwritten preallocated space, or a torn record from an unclean shutdown.
// endOffset is exactly how many leading bytes of the segment are valid
// data; recovery.go uses it to set segment.size.
func (s *segment) forEach(fromOffset int64, fn func(offset int64, h recordHeader, key, value []byte) error) (endOffset int64, err error) {
	data := s.mf.Bytes()
	limit := int64(len(data))
	off := fromOffset
	for off+headerSize <= limit {
		h := decodeHeader(data[off : off+headerSize])
		total := recordSize(h.keyLen, h.valLen)
		if total < headerSize || off+total > limit {
			break // torn tail: declared length overruns the segment
		}
		record := data[off : off+total]
		if !verifyChecksum(record) {
			break // corrupt record or unwritten (zeroed) space
		}
		key := record[headerSize : headerSize+int64(h.keyLen)]
		value := record[headerSize+int64(h.keyLen) : total]
		if err := fn(off, h, key, value); err != nil {
			return off, err
		}
		off += total
	}
	return off, nil
}

func (s *segment) sync() error {
	return s.mf.Sync()
}

// finalize shrinks the mapped capacity down to the segment's actual used
// size, reclaiming its unused preallocated tail. Called when a segment is
// rotated out and becomes immutable.
func (s *segment) finalize() error {
	if s.size == s.capacity {
		return nil
	}
	if err := s.mf.Truncate(s.size); err != nil {
		return fmt.Errorf("finalize segment %d: %w", s.id, err)
	}
	s.capacity = s.size
	return nil
}

func (s *segment) close() error {
	return s.mf.Close()
}

// remove closes and deletes the segment file, used once compaction has
// migrated all live records out of it.
func (s *segment) remove() error {
	if err := s.close(); err != nil {
		return err
	}
	return os.Remove(s.path)
}
