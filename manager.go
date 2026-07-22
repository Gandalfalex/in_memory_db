package kv

import (
	"fmt"
	"time"

	"github.com/Gandalfalex/in_memory_db/internal/pool"
)

// ManagedDBStat describes one named resource its manager has seen since
// it was created (entries are retained after an idle close, so a name
// reappears as Open=false rather than vanishing). Used by Manager,
// FileIndexManager, and SortedIndexManager.
type ManagedDBStat = pool.Stat

// ManagerOptions configures a Manager.
type ManagerOptions struct {
	// BaseDir is the directory under which each named DB gets its own
	// subdirectory (BaseDir/<name>). Required.
	BaseDir string
	// IdleTTL is how long a DB with no outstanding handles may sit unused
	// before the background reaper closes it, releasing its in-memory
	// index and mmap regions. The next Acquire transparently reopens it.
	// Zero or negative disables reaping (DBs stay open until Close).
	IdleTTL time.Duration
	// SweepInterval is how often the reaper checks for idle DBs. Defaults
	// to IdleTTL/4, but no more often than once per second. Ignored when
	// IdleTTL is zero.
	SweepInterval time.Duration
	// DBOptions, if non-nil, derives the Options for a named DB from its
	// name and assigned directory. The returned Options.Dir is always
	// overridden with dir so a misconfigured callback cannot alias two
	// names onto one directory. Defaults to DefaultOptions(dir).
	DBOptions func(name, dir string) Options
}

// Manager pools named DB instances under one base directory. Each DB is
// opened lazily on first Acquire and kept open while handles are
// outstanding; once its last handle is released and IdleTTL elapses, the
// background reaper closes it (writing an index snapshot per its Options,
// so reopening is cheap). This suits many datasets that are each touched
// rarely: on-disk state is permanent, RAM is paid only while a DB is in
// active use.
type Manager struct {
	p *pool.Pool[*DB]
}

// NewManager creates the base directory if needed and starts the idle
// reaper (when opts.IdleTTL > 0). No DBs are opened until Acquire.
func NewManager(opts ManagerOptions) (*Manager, error) {
	if opts.BaseDir == "" {
		return nil, fmt.Errorf("kv: ManagerOptions.BaseDir is required")
	}
	dbOptions := opts.DBOptions
	if dbOptions == nil {
		dbOptions = func(_, dir string) Options { return DefaultOptions(dir) }
	}
	open := func(name, dir string) (*DB, error) {
		o := dbOptions(name, dir)
		o.Dir = dir
		db, err := Open(o)
		if err != nil {
			return nil, fmt.Errorf("kv: open managed db %q: %w", name, err)
		}
		return db, nil
	}
	p, err := pool.New("db", opts.BaseDir, opts.IdleTTL, opts.SweepInterval, open)
	if err != nil {
		return nil, err
	}
	return &Manager{p: p}, nil
}

// Acquire returns a handle to the named DB, opening it first if it is not
// already open. Every Acquire must be paired with Handle.Release (or
// Handle.Close); the DB stays open — exempt from idle reaping — while any
// handle is outstanding.
func (m *Manager) Acquire(name string) (*Handle, error) {
	db, e, err := m.p.Acquire(name)
	if err != nil {
		return nil, err
	}
	return &Handle{DB: db, HandleRef: pool.HandleRef[*DB]{Entry: e}}, nil
}

// Handle is a refcounted reference to a managed DB. The embedded *DB
// exposes the full store API; Close is shadowed to mean Release, so
// "h, _ := m.Acquire(name); defer h.Close()" never closes the underlying
// DB out from under other handle holders.
type Handle struct {
	*DB
	pool.HandleRef[*DB]
}

// Close releases the handle (it does not close the underlying DB; the
// Manager owns that). It exists so a Handle satisfies the usual
// defer-Close idiom and always returns nil.
func (h *Handle) Close() error {
	h.Release()
	return nil
}

// Close closes every open DB and rejects further Acquires. Outstanding
// handles are invalidated: their operations return ErrClosed. A second
// call returns ErrManagerClosed.
func (m *Manager) Close() error { return m.p.Close() }

// Stats returns a snapshot of every known DB, sorted by name. Cheap: no
// I/O.
func (m *Manager) Stats() []ManagedDBStat { return m.p.Stats() }

// sweep is exposed for tests; the reaper goroutine calls the pool's sweep
// directly.
func (m *Manager) sweep(now time.Time) { m.p.Sweep(now) }
