// Package pool is the resource-agnostic core behind this module's three
// façade managers (Manager, FileIndexManager, SortedIndexManager, all at
// the module root): named resources under one base directory, opened
// lazily on Acquire, refcounted while handles are outstanding, and closed
// by a background reaper once idle for IdleTTL. The façades own the
// public API (naming, typed handles); Pool owns all the concurrency and
// lifecycle machinery, once, instead of three times.
package pool

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by Acquire after a Pool's Close has been called,
// and by a second call to Close.
var ErrClosed = errors.New("kv: manager is closed")

// Pool is generic over any resource with a Close method — *bitcask.DB,
// *fileindex.FileIndex, *sortedindex.SortedIndex all qualify as-is.
type Pool[T io.Closer] struct {
	kind    string // "db", "file index", "sorted index" — for error messages
	baseDir string
	idleTTL time.Duration
	// openRes opens the named resource rooted at path (a subpath of
	// baseDir assigned by the pool). It must wrap its own errors.
	openRes func(name, path string) (T, error)

	closed atomic.Bool

	mu      sync.Mutex
	entries map[string]*Entry[T]

	reaperStop chan struct{}
	reaperDone chan struct{}
}

// Entry is the pool's per-name state. Lock ordering: Pool.mu before
// Entry.mu, and Pool.mu is never acquired while holding Entry.mu.
type Entry[T io.Closer] struct {
	name string
	path string

	mu          sync.Mutex
	res         T
	live        bool // res is open; false while never opened or reaped
	refs        int
	idleSince   time.Time // when refs last dropped to 0; meaningful only then
	lastReapErr error     // error from the most recent idle close, if any
}

// New creates baseDir if needed and starts the idle reaper (when
// idleTTL > 0). No resources are opened until Acquire.
func New[T io.Closer](kind, baseDir string, idleTTL, sweepInterval time.Duration, openRes func(name, path string) (T, error)) (*Pool[T], error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("kv: create base dir: %w", err)
	}
	p := &Pool[T]{
		kind:    kind,
		baseDir: baseDir,
		idleTTL: idleTTL,
		openRes: openRes,
		entries: make(map[string]*Entry[T]),
	}
	if idleTTL > 0 {
		interval := sweepInterval
		if interval <= 0 {
			interval = max(idleTTL/4, time.Second)
		}
		p.startReaper(interval)
	}
	return p, nil
}

// ValidateName rejects names that would escape the base directory or
// collide with the flat one-path-per-name layout.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("kv: db name must not be empty")
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("kv: invalid db name %q: must be a single path element", name)
	}
	return nil
}

// Acquire returns the named resource (opening it first if needed) and its
// entry, with the entry's refcount already incremented. The caller must
// pair it with Entry.Release (typically via a HandleRef).
func (p *Pool[T]) Acquire(name string) (T, *Entry[T], error) {
	var zero T
	if err := ValidateName(name); err != nil {
		return zero, nil, err
	}
	if p.closed.Load() {
		return zero, nil, ErrClosed
	}

	p.mu.Lock()
	e := p.entries[name]
	if e == nil {
		e = &Entry[T]{name: name, path: filepath.Join(p.baseDir, name)}
		p.entries[name] = e
	}
	p.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check under e.mu: Pool.Close closes entries under this same lock,
	// so this ordering guarantees we never open a resource that Close's
	// sweep has already passed over.
	if p.closed.Load() {
		return zero, nil, ErrClosed
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

// Release drops one reference. When refs hit zero the idle clock starts;
// the reaper may close the resource any time after idleTTL.
func (e *Entry[T]) Release() {
	e.mu.Lock()
	e.refs--
	if e.refs == 0 {
		e.idleSince = time.Now()
	}
	e.mu.Unlock()
}

// Close closes every open resource and rejects further Acquires. A
// second call returns ErrClosed.
func (p *Pool[T]) Close() error {
	if p.closed.Swap(true) {
		return ErrClosed
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

// Stats returns a snapshot of every known entry, sorted by name.
func (p *Pool[T]) Stats() []Stat {
	entries := p.snapshotEntries()
	stats := make([]Stat, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		s := Stat{Name: e.name, Open: e.live, Refs: e.refs, LastReapErr: e.lastReapErr}
		if e.refs == 0 {
			s.IdleSince = e.idleSince
		}
		e.mu.Unlock()
		stats = append(stats, s)
	}
	slices.SortFunc(stats, func(a, b Stat) int { return strings.Compare(a.Name, b.Name) })
	return stats
}

// snapshotEntries copies the entry list out from under p.mu so per-entry
// locks are never taken while holding it.
func (p *Pool[T]) snapshotEntries() []*Entry[T] {
	p.mu.Lock()
	entries := make([]*Entry[T], 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	p.mu.Unlock()
	return entries
}

func (p *Pool[T]) startReaper(interval time.Duration) {
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
				p.Sweep(now)
			}
		}
	}()
}

func (p *Pool[T]) stopReaper() {
	if p.reaperStop == nil {
		return
	}
	close(p.reaperStop)
	<-p.reaperDone
}

// Sweep closes every open resource that has had no handles for at least
// idleTTL as of now. A failed close is recorded in LastReapErr but the
// resource is still marked closed (Close leaves it unusable regardless of
// its error), so the next Acquire reopens from disk. Exported so test
// code can trigger a sweep synchronously instead of waiting on the
// reaper's ticker.
func (p *Pool[T]) Sweep(now time.Time) {
	for _, e := range p.snapshotEntries() {
		e.mu.Lock()
		if e.live && e.refs == 0 && now.Sub(e.idleSince) >= p.idleTTL {
			e.lastReapErr = e.res.Close()
			e.live = false
		}
		e.mu.Unlock()
	}
}

// HandleRef is the refcounting half of a handle: idempotent Release tied
// to one Entry. Façade wrapper types (Handle, FileIndexHandle,
// SortedIndexHandle, all at the module root) embed it next to the
// resource pointer, constructing it from the Entry Acquire returned:
//
//	res, e, err := p.Acquire(name)
//	h := &Handle{DB: res, HandleRef: pool.HandleRef[*bitcask.DB]{Entry: e}}
type HandleRef[T io.Closer] struct {
	Entry    *Entry[T]
	released atomic.Bool
}

// Name returns the managed resource's name.
func (h *HandleRef[T]) Name() string { return h.Entry.name }

// Release drops this handle's reference. When the last handle for a name
// is released, its idle clock starts; the reaper may close the resource
// any time after the idle TTL. Release is idempotent; using the handle
// afterwards may return ErrClosed (the store's own, not this package's)
// once the reaper runs.
func (h *HandleRef[T]) Release() {
	if h.released.Swap(true) {
		return
	}
	h.Entry.Release()
}

// Stat describes one named resource its Pool has seen since it was
// created (entries are retained after an idle close, so a name reappears
// as Open=false rather than vanishing).
type Stat struct {
	// Name is the resource's name (its path element under baseDir).
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
