package kv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SortedIndexManagerOptions configures a SortedIndexManager.
type SortedIndexManagerOptions struct {
	// CacheDir is where each named dataset's built .sidx/.sparse/.bloom/
	// .sources sidecars are cached, one subdirectory per name
	// (CacheDir/<name>/index.sidx). Required. Source files themselves live
	// wherever the caller keeps them (passed to Acquire) and are never
	// written to.
	CacheDir string
	// KeyFunc extracts the index key from a line; shared by every managed
	// dataset. Required.
	KeyFunc KeyFunc
	// BuildOptions tunes builds triggered by a stale or missing cache; see
	// SortedIndexOptions.
	BuildOptions SortedIndexOptions
	// IdleTTL is how long a dataset with no outstanding handles may sit
	// open before the background reaper closes it — dropping its sparse
	// directory and Bloom filter from RAM and closing its file
	// descriptors, the point of this manager for a dataset that's queried
	// in rare bursts and otherwise idle. The next Acquire transparently
	// reopens: a cheap stat-and-reopen if the sources haven't changed
	// since the cache was built, a full rebuild if they have. Zero or
	// negative disables reaping.
	IdleTTL time.Duration
	// SweepInterval is how often the reaper checks for idle datasets.
	// Defaults to IdleTTL/4, but no more often than once per second.
	// Ignored when IdleTTL is zero.
	SweepInterval time.Duration
}

// SortedIndexManager pools named SortedIndex instances, each built (or
// reopened) on demand from a caller-supplied, caller-ordered list of
// source files, with the same lazy-open/refcount/idle-reap behavior as
// Manager and FileIndexManager (all three share pool.go's machinery).
//
// Unlike Manager/FileIndexManager, where a name maps to one fixed file
// under BaseDir, a SortedIndex is built from a set of files the caller
// tracks (e.g. a one-shot base plus incremental change files) — so
// Acquire takes that list explicitly every call. CacheDir only holds the
// built sidecars, not the source data.
type SortedIndexManager struct {
	p         *pool[*SortedIndex]
	keyFunc   KeyFunc
	buildOpts SortedIndexOptions

	mu      sync.Mutex
	sources map[string][]string // name -> most recently Acquired source list
}

// NewSortedIndexManager creates CacheDir if needed and starts the idle
// reaper (when opts.IdleTTL > 0). No datasets are built or opened until
// Acquire.
func NewSortedIndexManager(opts SortedIndexManagerOptions) (*SortedIndexManager, error) {
	if opts.CacheDir == "" {
		return nil, fmt.Errorf("kv: SortedIndexManagerOptions.CacheDir is required")
	}
	if opts.KeyFunc == nil {
		return nil, fmt.Errorf("kv: SortedIndexManagerOptions.KeyFunc is required")
	}

	m := &SortedIndexManager{
		keyFunc:   opts.KeyFunc,
		buildOpts: opts.BuildOptions,
		sources:   make(map[string][]string),
	}

	open := func(name, path string) (*SortedIndex, error) {
		m.mu.Lock()
		srcs := append([]string(nil), m.sources[name]...)
		m.mu.Unlock()
		if len(srcs) == 0 {
			return nil, fmt.Errorf("kv: no source files registered for %q: Acquire must be called with sourcePaths", name)
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("kv: create cache dir for %q: %w", name, err)
		}
		sidxPath := filepath.Join(path, "index.sidx")
		// pool.go's openRes has no ctx parameter (Manager/FileIndexManager
		// don't need one; DB.Open/OpenFileIndex are fast), so a build
		// triggered via Acquire can't be cancelled the way a direct
		// EnsureFresh/BuildSortedIndex call can. Call BuildSortedIndex or
		// EnsureFresh directly first (e.g. during a deploy step) if you
		// need a cancellable/time-bounded build before Acquire ever has to
		// trigger one.
		si, err := EnsureFresh(context.Background(), srcs, sidxPath, m.keyFunc, m.buildOpts)
		if err != nil {
			return nil, fmt.Errorf("kv: open managed sorted index %q: %w", name, err)
		}
		return si, nil
	}

	p, err := newPool("sorted index", opts.CacheDir, opts.IdleTTL, opts.SweepInterval, open)
	if err != nil {
		return nil, err
	}
	m.p = p
	return m, nil
}

// Acquire returns a handle to the named dataset's SortedIndex, building
// or reopening it as needed from sourcePaths — ordered lowest-to-highest
// precedence, e.g. []string{"report-files.jsonl", "report-changes.jsonl"}
// so a later file wins on a key conflict. sourcePaths is recorded for
// name and is what's used the next time this dataset needs (re)building,
// including after an idle reap; while a handle for name is already open,
// a changed sourcePaths on a subsequent Acquire is recorded but doesn't
// itself force a rebuild — that check happens at (re)open time, matching
// this manager's "rare bursts, otherwise idle" access pattern rather than
// live-updating.
//
// Every Acquire must be paired with Release (or Close); the index stays
// open — exempt from idle reaping — while any handle is outstanding.
func (m *SortedIndexManager) Acquire(name string, sourcePaths []string) (*SortedIndexHandle, error) {
	if len(sourcePaths) == 0 {
		return nil, fmt.Errorf("kv: Acquire requires at least one source path")
	}
	m.mu.Lock()
	m.sources[name] = sourcePaths
	m.mu.Unlock()

	si, e, err := m.p.acquire(name)
	if err != nil {
		return nil, err
	}
	return &SortedIndexHandle{SortedIndex: si, handleRef: handleRef[*SortedIndex]{entry: e}}, nil
}

// SortedIndexHandle is a refcounted reference to a managed SortedIndex.
// The embedded *SortedIndex exposes the full API; Close is shadowed to
// mean Release, so a deferred Close never closes the underlying index out
// from under other handle holders.
type SortedIndexHandle struct {
	*SortedIndex
	handleRef[*SortedIndex]
}

// Close releases the handle (it does not close the underlying
// SortedIndex; the SortedIndexManager owns that). Always returns nil.
func (h *SortedIndexHandle) Close() error {
	h.Release()
	return nil
}

// Close closes every open SortedIndex and rejects further Acquires.
// A second call returns ErrManagerClosed.
func (m *SortedIndexManager) Close() error { return m.p.close() }

// Stats returns a snapshot of every known dataset, sorted by name.
// Cheap: no I/O.
func (m *SortedIndexManager) Stats() []ManagedDBStat { return m.p.stats() }

// sweep is exposed for tests; the reaper goroutine calls the pool's sweep
// directly.
func (m *SortedIndexManager) sweep(now time.Time) { m.p.sweep(now) }
