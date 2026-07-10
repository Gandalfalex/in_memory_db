package kv

import (
	"fmt"
	"time"
)

// FileIndexManagerOptions configures a FileIndexManager.
type FileIndexManagerOptions struct {
	// BaseDir is the directory under which each named FileIndex gets its
	// own file (BaseDir/<name>; include any extension, e.g. "traces.jsonl",
	// in the name). Required.
	BaseDir string
	// KeyFunc extracts the index key from a line; shared by every managed
	// FileIndex. Required.
	KeyFunc KeyFunc
	// IdleTTL is how long a FileIndex with no outstanding handles may sit
	// unused before the background reaper closes it, releasing its
	// in-memory index. The next Acquire transparently reopens it (one
	// rebuild scan). Zero or negative disables reaping.
	IdleTTL time.Duration
	// SweepInterval is how often the reaper checks for idle indexes.
	// Defaults to IdleTTL/4, but no more often than once per second.
	// Ignored when IdleTTL is zero.
	SweepInterval time.Duration
}

// FileIndexManager pools named FileIndex instances under one base
// directory, with the same lazy-open/refcount/idle-reap behavior as
// Manager (both share one resource-agnostic pool underneath).
type FileIndexManager struct {
	p *pool[*FileIndex]
}

// NewFileIndexManager creates the base directory if needed and starts the
// idle reaper (when opts.IdleTTL > 0). No files are opened until Acquire.
func NewFileIndexManager(opts FileIndexManagerOptions) (*FileIndexManager, error) {
	if opts.BaseDir == "" {
		return nil, fmt.Errorf("kv: FileIndexManagerOptions.BaseDir is required")
	}
	if opts.KeyFunc == nil {
		return nil, fmt.Errorf("kv: FileIndexManagerOptions.KeyFunc is required")
	}
	open := func(name, path string) (*FileIndex, error) {
		fi, err := OpenFileIndex(path, opts.KeyFunc)
		if err != nil {
			return nil, fmt.Errorf("kv: open managed file index %q: %w", name, err)
		}
		return fi, nil
	}
	p, err := newPool("file index", opts.BaseDir, opts.IdleTTL, opts.SweepInterval, open)
	if err != nil {
		return nil, err
	}
	return &FileIndexManager{p: p}, nil
}

// Acquire returns a handle to the named FileIndex, opening it first if it
// is not already open. Every Acquire must be paired with Release (or
// Close); the index stays open — exempt from idle reaping — while any
// handle is outstanding.
func (m *FileIndexManager) Acquire(name string) (*FileIndexHandle, error) {
	fi, e, err := m.p.acquire(name)
	if err != nil {
		return nil, err
	}
	return &FileIndexHandle{FileIndex: fi, handleRef: handleRef[*FileIndex]{entry: e}}, nil
}

// FileIndexHandle is a refcounted reference to a managed FileIndex. The
// embedded *FileIndex exposes the full API; Close is shadowed to mean
// Release, so a deferred Close never closes the underlying index out from
// under other handle holders.
type FileIndexHandle struct {
	*FileIndex
	handleRef[*FileIndex]
}

// Close releases the handle (it does not close the underlying FileIndex;
// the FileIndexManager owns that). Always returns nil.
func (h *FileIndexHandle) Close() error {
	h.Release()
	return nil
}

// Close closes every open FileIndex and rejects further Acquires.
// Outstanding handles are invalidated: their operations return ErrClosed.
// A second call returns ErrManagerClosed.
func (m *FileIndexManager) Close() error { return m.p.close() }

// Stats returns a snapshot of every known FileIndex, sorted by name.
// Cheap: no I/O.
func (m *FileIndexManager) Stats() []ManagedDBStat { return m.p.stats() }

// sweep is exposed for tests; the reaper goroutine calls the pool's sweep
// directly.
func (m *FileIndexManager) sweep(now time.Time) { m.p.sweep(now) }
