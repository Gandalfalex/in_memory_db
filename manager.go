package kv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrManagerClosed is returned by Acquire after Manager.Close has been
// called, and by a second call to Manager.Close.
var ErrManagerClosed = errors.New("kv: manager is closed")

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
	opts   ManagerOptions
	closed atomic.Bool

	mu      sync.Mutex
	entries map[string]*managedDB

	reaperStop chan struct{}
	reaperDone chan struct{}
}

// managedDB is the Manager's per-name state. Lock ordering: Manager.mu
// before managedDB.mu, and Manager.mu is never acquired while holding
// managedDB.mu.
type managedDB struct {
	name string
	dir  string

	mu          sync.Mutex
	db          *DB // nil while closed (never opened, or reaped)
	refs        int
	idleSince   time.Time // when refs last dropped to 0; meaningful only then
	lastReapErr error     // error from the most recent idle close, if any
}

// NewManager creates the base directory if needed and starts the idle
// reaper (when opts.IdleTTL > 0). No DBs are opened until Acquire.
func NewManager(opts ManagerOptions) (*Manager, error) {
	if opts.BaseDir == "" {
		return nil, fmt.Errorf("kv: ManagerOptions.BaseDir is required")
	}
	if err := os.MkdirAll(opts.BaseDir, 0o755); err != nil {
		return nil, fmt.Errorf("kv: create base dir: %w", err)
	}
	if opts.DBOptions == nil {
		opts.DBOptions = func(_, dir string) Options { return DefaultOptions(dir) }
	}
	m := &Manager{opts: opts, entries: make(map[string]*managedDB)}
	if opts.IdleTTL > 0 {
		interval := opts.SweepInterval
		if interval <= 0 {
			interval = opts.IdleTTL / 4
			if interval < time.Second {
				interval = time.Second
			}
		}
		m.startReaper(interval)
	}
	return m, nil
}

// validateManagedName rejects names that would escape BaseDir or collide
// with the flat one-directory-per-name layout.
func validateManagedName(name string) error {
	if name == "" {
		return fmt.Errorf("kv: db name must not be empty")
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("kv: invalid db name %q: must be a single path element", name)
	}
	return nil
}

// Acquire returns a handle to the named DB, opening it first if it is not
// already open. Every Acquire must be paired with Handle.Release (or
// Handle.Close); the DB stays open — exempt from idle reaping — while any
// handle is outstanding.
func (m *Manager) Acquire(name string) (*Handle, error) {
	if err := validateManagedName(name); err != nil {
		return nil, err
	}
	if m.closed.Load() {
		return nil, ErrManagerClosed
	}

	m.mu.Lock()
	e := m.entries[name]
	if e == nil {
		e = &managedDB{name: name, dir: filepath.Join(m.opts.BaseDir, name)}
		m.entries[name] = e
	}
	m.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check under e.mu: Manager.Close closes entries under this same
	// lock, so this ordering guarantees we never open a DB that Close's
	// sweep has already passed over.
	if m.closed.Load() {
		return nil, ErrManagerClosed
	}
	if e.db == nil {
		opts := m.opts.DBOptions(name, e.dir)
		opts.Dir = e.dir
		db, err := Open(opts)
		if err != nil {
			return nil, fmt.Errorf("kv: open managed db %q: %w", name, err)
		}
		e.db = db
	}
	e.refs++
	return &Handle{DB: e.db, entry: e}, nil
}

// Handle is a refcounted reference to a managed DB. The embedded *DB
// exposes the full store API; Close is shadowed to mean Release, so
// "h, _ := m.Acquire(name); defer h.Close()" never closes the underlying
// DB out from under other handle holders.
type Handle struct {
	*DB
	entry    *managedDB
	released atomic.Bool
}

// Name returns the managed DB's name.
func (h *Handle) Name() string { return h.entry.name }

// Release drops this handle's reference. When the last handle for a name
// is released, its idle clock starts; the reaper may close the DB any
// time after IdleTTL. Release is idempotent; using the handle afterwards
// may return ErrClosed once the reaper runs.
func (h *Handle) Release() {
	if h.released.Swap(true) {
		return
	}
	e := h.entry
	e.mu.Lock()
	e.refs--
	if e.refs == 0 {
		e.idleSince = time.Now()
	}
	e.mu.Unlock()
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
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return ErrManagerClosed
	}
	m.stopReaper()

	m.mu.Lock()
	entries := make([]*managedDB, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.Unlock()

	var errs []error
	for _, e := range entries {
		e.mu.Lock()
		if e.db != nil {
			if err := e.db.Close(); err != nil {
				errs = append(errs, fmt.Errorf("kv: close managed db %q: %w", e.name, err))
			}
			e.db = nil
		}
		e.mu.Unlock()
	}
	return errors.Join(errs...)
}

// ManagedDBStat describes one named DB the Manager has seen since it was
// created (entries are retained after an idle close, so a name reappears
// as Open=false rather than vanishing).
type ManagedDBStat struct {
	// Name is the DB's name (its subdirectory under BaseDir).
	Name string
	// Open reports whether the DB is currently open (holding RAM).
	Open bool
	// Refs is the number of outstanding handles.
	Refs int
	// IdleSince is when Refs last dropped to zero. Zero-valued while
	// handles are outstanding or if the DB was never acquired.
	IdleSince time.Time
	// LastReapErr is the error from the most recent idle close, nil if it
	// succeeded. The DB is considered closed either way.
	LastReapErr error
}

// Stats returns a snapshot of every known DB, sorted by name. Cheap: no
// I/O.
func (m *Manager) Stats() []ManagedDBStat {
	m.mu.Lock()
	entries := make([]*managedDB, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.Unlock()

	stats := make([]ManagedDBStat, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		s := ManagedDBStat{Name: e.name, Open: e.db != nil, Refs: e.refs, LastReapErr: e.lastReapErr}
		if e.refs == 0 {
			s.IdleSince = e.idleSince
		}
		e.mu.Unlock()
		stats = append(stats, s)
	}
	slices.SortFunc(stats, func(a, b ManagedDBStat) int { return strings.Compare(a.Name, b.Name) })
	return stats
}

func (m *Manager) startReaper(interval time.Duration) {
	m.reaperStop = make(chan struct{})
	m.reaperDone = make(chan struct{})
	go func() {
		defer close(m.reaperDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.reaperStop:
				return
			case now := <-ticker.C:
				m.sweep(now)
			}
		}
	}()
}

func (m *Manager) stopReaper() {
	if m.reaperStop == nil {
		return
	}
	close(m.reaperStop)
	<-m.reaperDone
}

// sweep closes every open DB that has had no handles for at least IdleTTL
// as of now. A failed close is recorded in lastReapErr but the DB is
// still marked closed (DB.Close leaves the store unusable regardless of
// its error), so the next Acquire reopens from disk.
func (m *Manager) sweep(now time.Time) {
	m.mu.Lock()
	entries := make([]*managedDB, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.Unlock()

	for _, e := range entries {
		e.mu.Lock()
		if e.db != nil && e.refs == 0 && now.Sub(e.idleSince) >= m.opts.IdleTTL {
			e.lastReapErr = e.db.Close()
			e.db = nil
		}
		e.mu.Unlock()
	}
}
