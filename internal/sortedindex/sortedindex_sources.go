package sortedindex

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

const sourcesMagic = "KVSS"

func sourcesPathFor(sidxPath string) string { return sidxPath + ".sources" }

// sourceStat is one recorded source file's identity at build time: path
// plus size/mtime, enough to detect "this file changed" with a stat call,
// no content read.
type sourceStat struct {
	path    string
	size    int64
	modTime int64 // UnixNano
}

// statSources stats each path in order, erroring if any is missing —
// BuildSortedIndex and EnsureFresh both need every source to exist.
func statSources(paths []string) ([]sourceStat, error) {
	out := make([]sourceStat, len(paths))
	for i, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("kv: stat source %q: %w", p, err)
		}
		out[i] = sourceStat{path: p, size: info.Size(), modTime: info.ModTime().UnixNano()}
	}
	return out, nil
}

// sourcesMatch reports whether two stat snapshots describe the same
// ordered list of files: same paths, in the same precedence order, each
// unchanged in size and mtime.
func sourcesMatch(a, b []sourceStat) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].path != b[i].path || a[i].size != b[i].size || a[i].modTime != b[i].modTime {
			return false
		}
	}
	return true
}

// writeSourcesFile persists stats in one shot — tiny by construction, one
// entry per source file (typically a handful: a base file plus a few
// change files).
func writeSourcesFile(path string, stats []sourceStat) error {
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
	if _, err := io.WriteString(w, sourcesMagic); err != nil {
		return err
	}
	if err := kvtypes.WriteUint32(w, sidxVersion); err != nil {
		return err
	}
	if err := kvtypes.WriteUint64(w, uint64(len(stats))); err != nil {
		return err
	}
	for _, s := range stats {
		buf := make([]byte, 2+len(s.path)+8+8)
		binary.BigEndian.PutUint16(buf[0:2], uint16(len(s.path)))
		copy(buf[2:2+len(s.path)], s.path)
		o := 2 + len(s.path)
		binary.BigEndian.PutUint64(buf[o:o+8], uint64(s.size))
		binary.BigEndian.PutUint64(buf[o+8:o+16], uint64(s.modTime))
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

// readSourcesFile loads a stat snapshot fully into RAM — tiny by
// construction (one entry per source file).
func readSourcesFile(path string) ([]sourceStat, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("kv: open sources sidecar: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 4 {
		return nil, fmt.Errorf("kv: sources: truncated")
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
		return nil, fmt.Errorf("kv: sources: checksum mismatch")
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	magic := make([]byte, len(sourcesMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != sourcesMagic {
		return nil, fmt.Errorf("kv: sources: bad magic")
	}
	if _, err := kvtypes.ReadUint32(r); err != nil {
		return nil, err
	}
	count, err := kvtypes.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	out := make([]sourceStat, 0, count)
	for range count {
		pathLen, err := kvtypes.ReadUint16(r)
		if err != nil {
			return nil, err
		}
		pathBuf := make([]byte, pathLen)
		if _, err := io.ReadFull(r, pathBuf); err != nil {
			return nil, err
		}
		size, err := kvtypes.ReadUint64(r)
		if err != nil {
			return nil, err
		}
		modTime, err := kvtypes.ReadUint64(r)
		if err != nil {
			return nil, err
		}
		out = append(out, sourceStat{path: string(pathBuf), size: int64(size), modTime: int64(modTime)})
	}
	return out, nil
}

// EnsureFresh returns an open SortedIndex over sourcePaths (ordered
// lowest-to-highest precedence — later files win on a key conflict, e.g.
// a one-shot base file followed by incremental change files), updating
// sidxPath first only if it's missing or its recorded source stats
// (path/size/mtime) no longer match: a cheap stat-only check on the
// common path (warm cache, nothing changed), paying real work only when
// the data actually moved. This is the intended entry point for a
// rarely-queried, occasionally-changing dataset — see SortedIndexManager
// for pairing it with idle-TTL reaping.
//
// When something changed, EnsureFresh picks the cheapest safe option:
//   - if every previously recorded source is unchanged and the only
//     difference is new sources appended after them (incrementalEligible),
//     it folds just the new data into the existing sidx
//     (refreshSortedIndexLocked) — proportional to the new sources' size,
//     not the whole dataset;
//   - otherwise (a source changed, was removed, or was reordered) it
//     falls back to a full BuildSortedIndex.
//
// The stat check and any resulting build/refresh happen under the same
// per-path lock BuildSortedIndex uses (see buildLocks): this closes a
// TOCTOU that would otherwise exist between "checked fresh" and "decided
// what to do", and means a second concurrent EnsureFresh call for the
// same sidxPath typically does no redundant work — by the time it gets
// the lock, the first call has usually already made it fresh. ctx
// cancels in-progress work the same way BuildSortedIndex's does; it is
// not consulted once the (fast, read-only) Open step begins.
func EnsureFresh(ctx context.Context, sourcePaths []string, sidxPath string, keyFunc KeyFunc, opts SortedIndexOptions) (*SortedIndex, error) {
	mu := lockForBuild(sidxPath)
	mu.Lock()
	current, err := statSources(sourcePaths)
	if err != nil {
		mu.Unlock()
		return nil, err
	}

	recorded, recErr := readSourcesFile(sourcesPathFor(sidxPath))
	fresh := recErr == nil && sourcesMatch(current, recorded)

	if !fresh {
		var workErr error
		if newSrcs, ok := incrementalEligible(recorded, current); recErr == nil && ok {
			workErr = refreshSortedIndexLocked(ctx, recorded, newSrcs, sidxPath, keyFunc, opts)
		} else {
			workErr = buildSortedIndexLocked(ctx, sourcePaths, keyFunc, sidxPath, opts)
		}
		if workErr != nil {
			mu.Unlock()
			return nil, workErr
		}
	}
	mu.Unlock()
	return OpenSortedIndex(sidxPath, keyFunc)
}
