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

// ErrManagerClosed is returned by Acquire after a manager's Close has
// been called, and by a second call to Close. Shared by Manager and
// FileIndexManager.
var ErrManagerClosed = errors.New("kv: manager is closed")

// closer is the constraint shared by pooled resources (*DB, *FileIndex).
type closer interface{ Close() error }

// pool is the resource-agnostic core behind Manager and FileIndexManager:
// named resources under one base directory, opened lazily on acquire,
// refcounted while handles are outstanding, and closed by a background
// reaper once idle for idleTTL. The wrappers own the public API; pool
// owns all the machinery.
type pool[T closer] struct {
	kind    string // "db" or "file index", for error messages
	baseDir string
	idleTTL time.Duration
	// openRes opens the named resource rooted at path (a subpath of
	// baseDir assigned by the pool). It must wrap its own errors.
	openRes func(name, path string) (T, error)

	closed atomic.Bool

	mu      sync.Mutex
	entries map[string]*poolEntry[T]

	reaperStop chan struct{}
	reaperDone chan struct{}
}

// poolEntry is the pool's per-name state. Lock ordering: pool.mu before
// poolEntry.mu, and pool.mu is never acquired while holding poolEntry.mu.
type poolEntry[T closer] struct {
	name string
	path string

	mu          sync.Mutex
	res         T
	live        bool // res is open; false while never opened or reaped
	refs        int
	idleSince   time.Time // when refs last dropped to 0; meaningful only then
	lastReapErr error     // error from the most recent idle close, if any
}

// newPool creates baseDir if needed and starts the idle reaper (when
// idleTTL > 0). No resources are opened until acquire.
func newPool[T closer](kind, baseDir string, idleTTL, sweepInterval time.Duration, openRes func(name, path string) (T, error)) (*pool[T], error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("kv: create base dir: %w", err)
	}
	p := &pool[T]{
		kind:    kind,
		baseDir: baseDir,
		idleTTL: idleTTL,
		openRes: openRes,
		entries: make(map[string]*poolEntry[T]),
	}
	if idleTTL > 0 {
		interval := sweepInterval
		if interval <= 0 {
			interval = idleTTL / 4
			if interval < time.Second {
				interval = time.Second
			}
		}
		p.startReaper(interval)
	}
	return p, nil
}

// validateManagedName rejects names that would escape the base directory
// or collide with the flat one-path-per-name layout.
func validateManagedName(name string) error {
	if name == "" {
		return fmt.Errorf("kv: db name must not be empty")
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("kv: invalid db name %q: must be a single path element", name)
	}
	return nil
}

// acquire returns the named resource (opening it first if needed) and its
// entry, with the entry's refcount already incremented. The caller must
// pair it with entry.release.
func (p *pool[T]) acquire(name string) (T, *poolEntry[T], error) {
	var zero T
	if err := validateManagedName(name); err != nil {
		return zero, nil, err
	}
	if p.closed.Load() {
		return zero, nil, ErrManagerClosed
	}

	p.mu.Lock()
	e := p.entries[name]
	if e == nil {
		e = &poolEntry[T]{name: name, path: filepath.Join(p.baseDir, name)}
		p.entries[name] = e
	}
	p.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check under e.mu: pool.close closes entries under this same lock,
	// so this ordering guarantees we never open a resource that close's
	// sweep has already passed over.
	if p.closed.Load() {
		return zero, nil, ErrManagerClosed
	}
	if !e.live {
		res, err := p.openRes(name, e.path)
		if err != nil {
			return zero, nil, err
		}
		e.res, e.live = res, true
	}
	e.refs++
	return e.res, e, nil
}

// release drops one reference. When refs hit zero the idle clock starts;
// the reaper may close the resource any time after idleTTL.
func (e *poolEntry[T]) release() {
	e.mu.Lock()
	e.refs--
	if e.refs == 0 {
		e.idleSince = time.Now()
	}
	e.mu.Unlock()
}

// close closes every open resource and rejects further acquires. A second
// call returns ErrManagerClosed.
func (p *pool[T]) close() error {
	if p.closed.Swap(true) {
		return ErrManagerClosed
	}
	p.stopReaper()

	var errs []error
	for _, e := range p.snapshotEntries() {
		e.mu.Lock()
		if e.live {
			if err := e.res.Close(); err != nil {
				errs = append(errs, fmt.Errorf("kv: close managed %s %q: %w", p.kind, e.name, err))
			}
			e.live = false
		}
		e.mu.Unlock()
	}
	return errors.Join(errs...)
}

// stats returns a snapshot of every known entry, sorted by name.
func (p *pool[T]) stats() []ManagedDBStat {
	entries := p.snapshotEntries()
	stats := make([]ManagedDBStat, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		s := ManagedDBStat{Name: e.name, Open: e.live, Refs: e.refs, LastReapErr: e.lastReapErr}
		if e.refs == 0 {
			s.IdleSince = e.idleSince
		}
		e.mu.Unlock()
		stats = append(stats, s)
	}
	slices.SortFunc(stats, func(a, b ManagedDBStat) int { return strings.Compare(a.Name, b.Name) })
	return stats
}

// snapshotEntries copies the entry list out from under p.mu so per-entry
// locks are never taken while holding it.
func (p *pool[T]) snapshotEntries() []*poolEntry[T] {
	p.mu.Lock()
	entries := make([]*poolEntry[T], 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	p.mu.Unlock()
	return entries
}

func (p *pool[T]) startReaper(interval time.Duration) {
	p.reaperStop = make(chan struct{})
	p.reaperDone = make(chan struct{})
	go func() {
		defer close(p.reaperDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.reaperStop:
				return
			case now := <-ticker.C:
				p.sweep(now)
			}
		}
	}()
}

func (p *pool[T]) stopReaper() {
	if p.reaperStop == nil {
		return
	}
	close(p.reaperStop)
	<-p.reaperDone
}

// sweep closes every open resource that has had no handles for at least
// idleTTL as of now. A failed close is recorded in lastReapErr but the
// resource is still marked closed (Close leaves it unusable regardless of
// its error), so the next acquire reopens from disk.
func (p *pool[T]) sweep(now time.Time) {
	for _, e := range p.snapshotEntries() {
		e.mu.Lock()
		if e.live && e.refs == 0 && now.Sub(e.idleSince) >= p.idleTTL {
			e.lastReapErr = e.res.Close()
			e.live = false
		}
		e.mu.Unlock()
	}
}

// handleRef is the refcounting half of a handle: idempotent Release tied
// to one poolEntry. Wrappers embed it next to the resource pointer.
type handleRef[T closer] struct {
	entry    *poolEntry[T]
	released atomic.Bool
}

// Name returns the managed resource's name.
func (h *handleRef[T]) Name() string { return h.entry.name }

// Release drops this handle's reference. When the last handle for a name
// is released, its idle clock starts; the reaper may close the resource
// any time after the idle TTL. Release is idempotent; using the handle
// afterwards may return ErrClosed once the reaper runs.
func (h *handleRef[T]) Release() {
	if h.released.Swap(true) {
		return
	}
	h.entry.release()
}

// ManagedDBStat describes one named resource its manager has seen since
// it was created (entries are retained after an idle close, so a name
// reappears as Open=false rather than vanishing). Used by both Manager
// and FileIndexManager.
type ManagedDBStat struct {
	// Name is the resource's name (its path element under BaseDir).
	Name string
	// Open reports whether the resource is currently open (holding RAM).
	Open bool
	// Refs is the number of outstanding handles.
	Refs int
	// IdleSince is when Refs last dropped to zero. Zero-valued while
	// handles are outstanding or if the resource was never acquired.
	IdleSince time.Time
	// LastReapErr is the error from the most recent idle close, nil if it
	// succeeded. The resource is considered closed either way.
	LastReapErr error
}
