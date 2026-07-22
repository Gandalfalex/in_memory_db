package sortedindex

// BuildSortedIndex and its external-merge-sort machinery: scan sources
// into RAM-bounded sorted runs (spillSortedRuns), then k-way merge them
// into the final sidx/sparse/Bloom files (mergeRuns). See SortedIndex's
// doc comment in sortedindex.go for the on-disk format this writes.

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

// maxSortedIndexSources bounds how many source files one SortedIndex can
// merge — fileIdx is stored as uint16 in both the run and sidx formats.
// Built for a small, human-managed stack of files (a one-shot base plus a
// handful of change files), not thousands of shards.
const maxSortedIndexSources = 1<<16 - 1

// ctxCheckInterval is how many scanned/merged entries pass between
// context cancellation checks in the scan and merge loops: frequent
// enough that a cancelled build stops within a bounded number of entries
// rather than running to completion regardless, infrequent enough that
// checking isn't a measurable per-entry cost at 78M-line scale.
const ctxCheckInterval = 100_000

// buildLocks serializes BuildSortedIndex (and EnsureFresh's build path)
// per sidxPath, within this process: two goroutines racing to (re)build
// the same cache would otherwise collide on the fixed temp file names
// (spillSortedRuns/mergeRuns use no PID or random suffix) and corrupt
// each other's output, or — even with unique temp names — complete out
// of order and have the stale one's rename land after the fresh one's,
// silently reverting the cache. Cross-process concurrent builds against
// a shared CacheDir are out of scope, consistent with the rest of this
// package (see doc.go: "an embedded, single-process key-value store");
// use a single process (e.g. one SortedIndexManager) as the owner of a
// given CacheDir if multiple callers need to trigger builds.
var buildLocks sync.Map // absolute sidxPath -> *sync.Mutex

func lockForBuild(sidxPath string) *sync.Mutex {
	key := sidxPath
	if abs, err := filepath.Abs(sidxPath); err == nil {
		key = abs
	}
	v, _ := buildLocks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// sortEntry is one (key, source location) pair, held in RAM only during a
// chunk sort and while spooled to/read from a run file. fileIdx is the
// entry's position in BuildSortedIndex's sourcePaths — the precedence a
// duplicate key is resolved by.
type sortEntry struct {
	key     []byte
	fileIdx uint16
	srcOff  int64
	length  int32
}

// BuildSortedIndex scans sourcePaths once each, in order, sequentially
// (same accept/skip rules as OpenFileIndex's rebuild: keyFunc rejecting a
// line skips it) and writes a lexicographically sorted directory to
// sidxPath (plus its sparse/Bloom/sources sidecars) via external merge
// sort: RAM at any point is bounded by opts.ChunkEntries, never by the
// combined source line count, so this scales to sources far larger than
// available RAM.
//
// sourcePaths is precedence order, lowest first: when the same key
// appears in more than one file (or more than once in the same file), the
// entry from the latest file wins, and within one file the latest line
// wins — a later change file always overrides the base, regardless of
// byte offset. This is the direct multi-file generalization of
// FileIndex's single-file "last line wins".
//
// No source file is ever rewritten. sidxPath and its sidecars are created
// fresh (or replaced if present); temporary run files are written
// alongside sidxPath and removed before return, including on error.
//
// Concurrent BuildSortedIndex calls (or EnsureFresh calls that decide to
// build) for the same sidxPath, within this process, are serialized —
// see buildLocks. ctx is checked periodically during the scan and merge
// passes (every ctxCheckInterval entries); a cancelled build stops
// promptly, cleans up its temp files the same as any other error, and
// returns ctx.Err() wrapped.
func BuildSortedIndex(ctx context.Context, sourcePaths []string, keyFunc KeyFunc, sidxPath string, opts SortedIndexOptions) error {
	mu := lockForBuild(sidxPath)
	mu.Lock()
	defer mu.Unlock()
	return buildSortedIndexLocked(ctx, sourcePaths, keyFunc, sidxPath, opts)
}

// buildSortedIndexLocked is BuildSortedIndex's body, factored out so
// EnsureFresh (sortedindex_sources.go) can hold buildLocks across its
// whole stat-then-maybe-build sequence — avoiding both a TOCTOU race
// against a concurrent build and the deadlock a second Lock() from
// inside BuildSortedIndex itself would cause. Callers must hold
// lockForBuild(sidxPath) already.
func buildSortedIndexLocked(ctx context.Context, sourcePaths []string, keyFunc KeyFunc, sidxPath string, opts SortedIndexOptions) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kv: build sorted index: %w", err)
	}
	if len(sourcePaths) == 0 {
		return fmt.Errorf("kv: BuildSortedIndex requires at least one source path")
	}
	if len(sourcePaths) > maxSortedIndexSources {
		return fmt.Errorf("kv: BuildSortedIndex supports at most %d source files", maxSortedIndexSources)
	}
	opts = opts.withDefaults()

	stats, err := statSources(sourcePaths)
	if err != nil {
		return err
	}

	runPaths, scanned, err := spillSortedRuns(ctx, sourcePaths, keyFunc, opts.ChunkEntries, sidxPath, 0)
	defer func() {
		for _, p := range runPaths {
			os.Remove(p)
		}
	}()
	if err != nil {
		return err
	}

	if err := mergeRuns(ctx, runPaths, "", sidxPath, opts.SparseInterval, scanned, opts.BloomFPR); err != nil {
		return err
	}
	return writeSourcesFile(sourcesPathFor(sidxPath), stats)
}

// spillSortedRuns scans each of sourcePaths in order, in chunks of
// chunkEntries lines spanning file boundaries, sorting each chunk in RAM
// and writing it to its own temporary run file (already sorted, so the
// merge phase never re-sorts). baseFileIdx is added to each source's
// position to get its fileIdx — 0 for a full BuildSortedIndex (indices
// 0..len(sourcePaths)-1), or len(recorded) for an incremental refresh
// (sortedindex_refresh.go), so newly scanned sources continue the
// existing sidx's fileIdx numbering rather than colliding with it.
// Returns the run file paths in creation order (the caller is
// responsible for removing them once the merge is done) and the total
// number of accepted lines scanned across every source — an upper bound
// on the final (post-dedup) key count, used only to size the Bloom
// filter; sizing high just spends a few more RAM bytes on the filter,
// never breaks correctness.
func spillSortedRuns(ctx context.Context, sourcePaths []string, keyFunc KeyFunc, chunkEntries int, sidxPath string, baseFileIdx int) ([]string, int64, error) {
	var runPaths []string
	var scanned int64
	chunk := make([]sortEntry, 0, chunkEntries)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.Slice(chunk, func(i, j int) bool { return bytes.Compare(chunk[i].key, chunk[j].key) < 0 })
		runPath := fmt.Sprintf("%s.run%d.tmp", sidxPath, len(runPaths))
		if err := writeRun(runPath, chunk); err != nil {
			return err
		}
		runPaths = append(runPaths, runPath)
		chunk = chunk[:0]
		return nil
	}

	for i, sourcePath := range sourcePaths {
		fileIdx := baseFileIdx + i
		err := scanOneSource(ctx, sourcePath, uint16(fileIdx), keyFunc, func(e sortEntry) error {
			chunk = append(chunk, e)
			scanned++
			if len(chunk) >= chunkEntries {
				return flush()
			}
			return nil
		})
		if err != nil {
			return runPaths, scanned, err
		}
	}
	if err := flush(); err != nil {
		return runPaths, scanned, err
	}
	return runPaths, scanned, nil
}

// scanOneSource sequentially reads sourcePath and calls add for every
// line keyFunc accepts, checking ctx every ctxCheckInterval lines.
func scanOneSource(ctx context.Context, sourcePath string, fileIdx uint16, keyFunc KeyFunc, add func(sortEntry) error) error {
	f, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("kv: open source %q: %w", sourcePath, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	var lines int64
	for {
		line, err := r.ReadBytes('\n')
		payload := line
		terminated := len(payload) > 0 && payload[len(payload)-1] == '\n'
		if terminated {
			payload = payload[:len(payload)-1]
		}
		if len(payload) > 0 {
			if key, ok := keyFunc(payload); ok && len(key) > 0 {
				keyCopy := append([]byte(nil), key...)
				if aerr := add(sortEntry{key: keyCopy, fileIdx: fileIdx, srcOff: off, length: int32(len(payload))}); aerr != nil {
					return aerr
				}
			}
		}
		off += int64(len(line))
		lines++
		if lines%ctxCheckInterval == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return fmt.Errorf("kv: build sorted index: %w", cerr)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("kv: scan source %q: %w", sourcePath, err)
		}
	}
}

// writeRun writes already-sorted entries to path as
// keyLen(2)|fileIdx(2)|srcOff(8)|length(4)|key records, no header (the
// merge phase reads until EOF).
func writeRun(path string, entries []sortEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	for _, e := range entries {
		if err := writeRunEntry(w, e); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func writeRunEntry(w io.Writer, e sortEntry) error {
	var hdr [16]byte
	binary.BigEndian.PutUint16(hdr[0:2], uint16(len(e.key)))
	binary.BigEndian.PutUint16(hdr[2:4], e.fileIdx)
	binary.BigEndian.PutUint64(hdr[4:12], uint64(e.srcOff))
	binary.BigEndian.PutUint32(hdr[12:16], uint32(e.length))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(e.key)
	return err
}

// runReader streams sortEntry records back out of a run file written by
// writeRun, in original (already-sorted) order.
type runReader struct {
	f *os.File
	r *bufio.Reader
}

func openRunReader(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &runReader{f: f, r: bufio.NewReaderSize(f, 1<<20)}, nil
}

func (rr *runReader) next() (sortEntry, error) {
	var hdr [16]byte
	if _, err := io.ReadFull(rr.r, hdr[:]); err != nil {
		return sortEntry{}, err // io.EOF at a clean boundary, or a real error
	}
	keyLen := binary.BigEndian.Uint16(hdr[0:2])
	fileIdx := binary.BigEndian.Uint16(hdr[2:4])
	srcOff := int64(binary.BigEndian.Uint64(hdr[4:12]))
	length := int32(binary.BigEndian.Uint32(hdr[12:16]))
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(rr.r, key); err != nil {
		return sortEntry{}, fmt.Errorf("kv: truncated run file: %w", err)
	}
	return sortEntry{key: key, fileIdx: fileIdx, srcOff: srcOff, length: length}, nil
}

func (rr *runReader) Close() error { return rr.f.Close() }

// entryReader is what mergeRuns consumes: an ordered stream of
// already-sorted sortEntry values. *runReader (a freshly spilled, not
// yet indexed run) is the usual implementation; sortedindex_refresh.go's
// *sidxRunReader (an existing sidx's entries, reused wholesale during an
// incremental refresh instead of a full rebuild) is the other.
type entryReader interface {
	next() (sortEntry, error)
	Close() error
}

func closeEntryReaders(readers []entryReader) {
	for _, r := range readers {
		if r != nil {
			r.Close()
		}
	}
}

// mergeHeap is a min-heap of run heads, ordered by key.
type mergeHeap []heapItem

type heapItem struct {
	entry  sortEntry
	runIdx int
}

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return bytes.Compare(h[i].entry.key, h[j].entry.key) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// mergeRuns k-way merges runPaths' sorted run files — plus, if
// existingSidxPath is non-empty, that sidx's own entries treated as one
// more already-sorted input (see sortedindex_refresh.go) — into the
// final sidx file at outSidxPath, plus its sparse and Bloom-filter
// sidecars. On a duplicate key across inputs, the entry from the higher
// fileIdx wins (a later source file always overrides an earlier one,
// and existingSidxPath's entries keep whatever lower fileIdx values they
// already had, so newly merged sources — always given higher fileIdx —
// correctly take precedence over it); if the duplicates share a fileIdx
// (two lines for the same key within one source file), the larger srcOff
// wins — the same "last line wins" rule OpenFileIndex's rebuild applies,
// generalized across files and across an incremental refresh.
// scannedUpperBound sizes the Bloom filter (an over-estimate is
// harmless, see spillSortedRuns); bloomFPR < 0 skips building one. ctx
// is checked every ctxCheckInterval entries.
//
// existingSidxPath may be the same path as outSidxPath (the normal
// incremental-refresh case: reopen sidxPath for reading here, then
// overwrite it in place at the end) — safe because the read is done via
// a file descriptor opened once up front, and the final os.Rename onto
// outSidxPath doesn't invalidate an already-open descriptor to the old
// file it replaces.
func mergeRuns(ctx context.Context, runPaths []string, existingSidxPath, outSidxPath string, sparseInterval int, scannedUpperBound int64, bloomFPR float64) error {
	readers := make([]entryReader, 0, len(runPaths)+1)
	// A closure, not defer closeEntryReaders(readers) directly: deferred
	// arguments are evaluated at the defer statement, which would freeze
	// on today's (empty) readers instead of whatever's been appended to
	// it by the time this function returns.
	defer func() { closeEntryReaders(readers) }()
	for _, p := range runPaths {
		r, err := openRunReader(p)
		if err != nil {
			return err
		}
		readers = append(readers, r)
	}
	if existingSidxPath != "" {
		r, err := openSidxRunReader(existingSidxPath)
		if err != nil {
			return err
		}
		readers = append(readers, r)
	}

	h := &mergeHeap{}
	heap.Init(h)
	for i, r := range readers {
		e, err := r.next()
		if err == nil {
			heap.Push(h, heapItem{e, i})
		} else if !errors.Is(err, io.EOF) {
			return fmt.Errorf("kv: read run: %w", err)
		}
	}

	bodyPath := outSidxPath + ".body.tmp"
	bodyF, err := os.Create(bodyPath)
	if err != nil {
		return err
	}
	defer func() {
		bodyF.Close()
		os.Remove(bodyPath)
	}()
	bw := bufio.NewWriterSize(bodyF, 1<<20)
	crc := crc32.NewIEEE()
	w := io.MultiWriter(bw, crc)

	var sparse []sparseEntry
	var count int64
	var entryOff int64 // relative to entries region; header is fixed-size, see below

	var bloom *bloomFilter
	if bloomFPR >= 0 {
		bloom = newBloomFilter(scannedUpperBound, bloomFPR)
	}

	advance := func(runIdx int) error {
		e, err := readers[runIdx].next()
		if err == nil {
			heap.Push(h, heapItem{e, runIdx})
		} else if !errors.Is(err, io.EOF) {
			return fmt.Errorf("kv: read run: %w", err)
		}
		return nil
	}

	const headerLen = int64(len(sidxMagic) + 4 + 8) // magic + version + count

	betterDup := func(candidate, current sortEntry) bool {
		if candidate.fileIdx != current.fileIdx {
			return candidate.fileIdx > current.fileIdx
		}
		return candidate.srcOff > current.srcOff
	}

	for h.Len() > 0 {
		if count%ctxCheckInterval == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return fmt.Errorf("kv: build sorted index: %w", cerr)
			}
		}
		top := heap.Pop(h).(heapItem)
		winner := top.entry
		if err := advance(top.runIdx); err != nil {
			return err
		}
		for h.Len() > 0 && bytes.Equal((*h)[0].entry.key, winner.key) {
			dup := heap.Pop(h).(heapItem)
			if betterDup(dup.entry, winner) {
				winner = dup.entry
			}
			if err := advance(dup.runIdx); err != nil {
				return err
			}
		}

		if count%int64(sparseInterval) == 0 {
			sparse = append(sparse, sparseEntry{
				key:    append([]byte(nil), winner.key...),
				offset: headerLen + entryOff,
			})
		}
		if bloom != nil {
			bloom.add(winner.key)
		}
		n, err := writeSidxEntry(w, winner)
		if err != nil {
			return err
		}
		entryOff += int64(n)
		count++
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := bodyF.Sync(); err != nil {
		return err
	}

	if err := assembleSidx(outSidxPath, bodyPath, count, crc.Sum32()); err != nil {
		return err
	}
	if err := writeSparseFile(sparsePathFor(outSidxPath), sparse); err != nil {
		return err
	}
	if bloom == nil {
		return nil
	}
	return writeBloomFile(bloomPathFor(outSidxPath), bloom)
}

func writeSidxEntry(w io.Writer, e sortEntry) (int, error) {
	buf := make([]byte, 2+len(e.key)+2+8+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(e.key)))
	copy(buf[2:2+len(e.key)], e.key)
	o := 2 + len(e.key)
	binary.BigEndian.PutUint16(buf[o:o+2], e.fileIdx)
	binary.BigEndian.PutUint64(buf[o+2:o+10], uint64(e.srcOff))
	binary.BigEndian.PutUint32(buf[o+10:o+14], uint32(e.length))
	return w.Write(buf)
}

// assembleSidx writes the final sidxPath: header (now that count is
// known) + the spooled entry body + trailing crc32.
func assembleSidx(sidxPath, bodyPath string, count int64, entriesCRC uint32) error {
	tmpPath := sidxPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		out.Close()
		if !ok {
			os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(out, 1<<20)
	if _, err := io.WriteString(bw, sidxMagic); err != nil {
		return err
	}
	if err := kvtypes.WriteUint32(bw, sidxVersion); err != nil {
		return err
	}
	if err := kvtypes.WriteUint64(bw, uint64(count)); err != nil {
		return err
	}
	body, err := os.Open(bodyPath)
	if err != nil {
		return err
	}
	defer body.Close()
	if _, err := io.Copy(bw, body); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.BigEndian, entriesCRC); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	ok = true
	return os.Rename(tmpPath, sidxPath)
}

// writeSparseFile writes the full in-RAM sparse directory (small: one
// entry per SparseInterval keys) to path in one shot.
func writeSparseFile(path string, sparse []sparseEntry) error {
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
	if _, err := io.WriteString(w, sparseMagic); err != nil {
		return err
	}
	if err := kvtypes.WriteUint32(w, sidxVersion); err != nil {
		return err
	}
	if err := kvtypes.WriteUint64(w, uint64(len(sparse))); err != nil {
		return err
	}
	for _, e := range sparse {
		buf := make([]byte, 2+len(e.key)+8)
		binary.BigEndian.PutUint16(buf[0:2], uint16(len(e.key)))
		copy(buf[2:2+len(e.key)], e.key)
		binary.BigEndian.PutUint64(buf[2+len(e.key):], uint64(e.offset))
		if _, err := w.Write(buf); err != nil {
			return err
		}
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
