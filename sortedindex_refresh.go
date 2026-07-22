package kv

// Incremental refresh: when EnsureFresh finds that every previously
// recorded source is byte-for-byte unchanged and the only difference is
// one or more new sources appended after them, it folds just the new
// data in — reusing the existing sidx's already-sorted entries as one
// more input to the same k-way merge BuildSortedIndex uses (see
// mergeRuns), instead of re-scanning and re-sorting everything from
// scratch. Cost is a single sequential pass over the existing sidx plus
// scanning/sorting only the new sources, rather than O(every source,
// re-parsed and re-sorted).
//
// This does not attempt to detect "an existing source grew via append"
// (e.g. a change file itself being appended to over time) — that would
// require trusting that the file's prior bytes are unchanged from a
// stat alone, which isn't verifiable without re-reading them, and a
// wrong guess would silently corrupt the existing sidx's entries for
// that file (their (fileIdx, srcOffset, length) would point at content
// that's no longer what it was). The strictly-safer, still valuable
// case handled here — a wholly new file appended to the precedence
// chain — needs no such assumption: nothing about any previously
// recorded source is touched.
//
// This is the same zero-copy tradeoff FileIndex's doc comment already
// describes for the DB-vs-FileIndex split: SortedIndex entries are
// pointers into the caller's source files, not copies of their content,
// so a caller that ever rewrites a recorded source's bytes in place
// (pathological — the whole design assumes append-only sources) gets
// whatever's currently at that offset on the next Get, whether the index
// was built incrementally or from scratch. Incremental refresh doesn't
// weaken that guarantee; it just also avoids re-verifying it against
// content it isn't going to touch either way — only against the
// path/size/mtime stat, same signal EnsureFresh's non-incremental path
// already trusted.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
)

// incrementalEligible reports whether current is exactly recorded with
// one or more new entries appended at the end: every previously known
// source is unchanged (same path, size, mtime), and nothing was removed,
// reordered, or modified. This is the only condition under which the
// existing sidx's entries are known-safe to reuse wholesale — their
// (fileIdx, srcOffset, length) values still point at valid, unchanged
// bytes. Anything else (a source removed, reordered, or its stat
// changed) returns ok=false, and the caller must fall back to a full
// BuildSortedIndex.
func incrementalEligible(recorded, current []sourceStat) (newSources []sourceStat, ok bool) {
	if len(current) <= len(recorded) {
		return nil, false
	}
	for i, r := range recorded {
		c := current[i]
		if c.path != r.path || c.size != r.size || c.modTime != r.modTime {
			return nil, false
		}
	}
	return current[len(recorded):], true
}

// refreshSortedIndexLocked folds newSrcs into sidxPath's existing
// entries: newSrcs are scanned and sorted the normal way (spillSortedRuns),
// given fileIdx values continuing after recorded's (so they correctly
// take precedence on any key collision — see mergeRuns), then merged
// against sidxPath's own entries, reused unmodified via a sidxRunReader.
// recorded+newSrcs together become the new .sources record. Callers must
// hold lockForBuild(sidxPath) and must have already confirmed
// incrementalEligible(recorded, append(recorded, newSrcs...)).
func refreshSortedIndexLocked(ctx context.Context, recorded, newSrcs []sourceStat, sidxPath string, keyFunc KeyFunc, opts SortedIndexOptions) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kv: refresh sorted index: %w", err)
	}
	if len(recorded)+len(newSrcs) > maxSortedIndexSources {
		return fmt.Errorf("kv: refresh sorted index: total source count would exceed %d", maxSortedIndexSources)
	}
	opts = opts.withDefaults()

	newPaths := make([]string, len(newSrcs))
	for i, s := range newSrcs {
		newPaths[i] = s.path
	}

	runPaths, scannedNew, err := spillSortedRuns(ctx, newPaths, keyFunc, opts.ChunkEntries, sidxPath, len(recorded))
	defer func() {
		for _, p := range runPaths {
			os.Remove(p)
		}
	}()
	if err != nil {
		return err
	}

	existingCount, err := sidxHeaderCount(sidxPath)
	if err != nil {
		return err
	}

	if err := mergeRuns(ctx, runPaths, sidxPath, sidxPath, opts.SparseInterval, existingCount+scannedNew, opts.BloomFPR); err != nil {
		return err
	}

	allStats := make([]sourceStat, 0, len(recorded)+len(newSrcs))
	allStats = append(allStats, recorded...)
	allStats = append(allStats, newSrcs...)
	return writeSourcesFile(sourcesPathFor(sidxPath), allStats)
}

// sidxHeaderCount reads just a sidx file's header-recorded entry count
// (no full checksum pass — that happens separately, once, when
// openSidxRunReader opens the same file for the actual merge read) —
// used only to size the Bloom filter's upper bound before the merge
// starts.
func sidxHeaderCount(sidxPath string) (int64, error) {
	f, err := os.Open(sidxPath)
	if err != nil {
		return 0, fmt.Errorf("kv: open sidx: %w", err)
	}
	defer f.Close()
	return verifySidxHeader(f)
}

// sidxRunReader streams an existing sidx file's entries back out as
// sortEntry values, in their original (already sorted) order — see the
// entryReader doc comment in sortedindex_build.go.
type sidxRunReader struct {
	f *os.File
	r *bufio.Reader
}

// openSidxRunReader opens sidxPath for a merge read: verifies its header
// and full checksum first (a corrupt existing sidx must never be merged
// from — that would silently propagate the corruption into the refreshed
// result), then positions the reader at the start of the entries region.
func openSidxRunReader(sidxPath string) (*sidxRunReader, error) {
	f, err := os.Open(sidxPath)
	if err != nil {
		return nil, fmt.Errorf("kv: open sidx: %w", err)
	}
	if _, err := verifySidxHeader(f); err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := verifySidxChecksum(f, info.Size()); err != nil {
		f.Close()
		return nil, err
	}
	entriesStart := int64(len(sidxMagic) + 4 + 8)
	entriesEnd := info.Size() - 4 // exclude trailing crc32
	// Bounded to the entries region, same as Get/Prefix's io.NewSectionReader
	// use in sortedindex.go: without this, readSidxEntry would run past the
	// last real entry into the crc32 trailer bytes and choke trying to parse
	// them as one more entry, instead of cleanly hitting io.EOF.
	sr := io.NewSectionReader(f, entriesStart, entriesEnd-entriesStart)
	return &sidxRunReader{f: f, r: bufio.NewReaderSize(sr, 1<<20)}, nil
}

func (sr *sidxRunReader) next() (sortEntry, error) {
	e, err := readSidxEntry(sr.r)
	if err != nil {
		return sortEntry{}, err // io.EOF at a clean entry boundary, or a real error
	}
	return sortEntry{key: e.key, fileIdx: e.fileIdx, srcOff: e.srcOff, length: e.length}, nil
}

func (sr *sidxRunReader) Close() error { return sr.f.Close() }
