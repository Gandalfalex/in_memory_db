package kv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testManager(t *testing.T, ttl time.Duration) *Manager {
	t.Helper()
	m, err := NewManager(ManagerOptions{
		BaseDir: t.TempDir(),
		IdleTTL: ttl,
		DBOptions: func(_, dir string) Options {
			opts := DefaultOptions(dir)
			opts.SegmentSize = 4096
			return opts
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func statFor(t *testing.T, m *Manager, name string) ManagedDBStat {
	t.Helper()
	for _, s := range m.Stats() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no stats entry for %q", name)
	return ManagedDBStat{}
}

func TestManagerAcquireReleaseRoundtrip(t *testing.T) {
	m := testManager(t, 0)

	h, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	if h.Name() != "logs" {
		t.Fatalf("Name() = %q", h.Name())
	}
	if err := h.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	h.Release()

	h2, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	got, err := h2.Get([]byte("k"))
	if err != nil || string(got) != "v" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

func TestManagerNamesAreIsolated(t *testing.T) {
	m := testManager(t, 0)

	a, err := m.Acquire("a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := m.Acquire("b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Put([]byte("k"), []byte("from-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound in b, got %v", err)
	}
}

func TestManagerSameDBWhileHandlesOutstanding(t *testing.T) {
	m := testManager(t, 0)

	h1, err := m.Acquire("x")
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	h2, err := m.Acquire("x")
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	if h1.DB != h2.DB {
		t.Fatal("expected both handles to share one *DB")
	}
	if s := statFor(t, m, "x"); s.Refs != 2 || !s.Open {
		t.Fatalf("stats = %+v", s)
	}
}

func TestManagerSweepClosesIdleAndReopens(t *testing.T) {
	const ttl = time.Minute
	m := testManager(t, ttl)

	h, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Put([]byte("k"), []byte("survives reap")); err != nil {
		t.Fatal(err)
	}
	h.Release()

	// Not yet idle long enough: must stay open.
	m.sweep(time.Now())
	if s := statFor(t, m, "logs"); !s.Open {
		t.Fatal("swept before IdleTTL elapsed")
	}

	m.sweep(time.Now().Add(ttl + time.Second))
	s := statFor(t, m, "logs")
	if s.Open {
		t.Fatal("expected idle DB to be closed")
	}
	if s.LastReapErr != nil {
		t.Fatalf("reap close failed: %v", s.LastReapErr)
	}

	// Stale handle now reports closed.
	if _, err := h.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed via stale handle, got %v", err)
	}

	// Reacquire reopens from disk with data intact.
	h2, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	got, err := h2.Get([]byte("k"))
	if err != nil || string(got) != "survives reap" {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}

func TestManagerSweepSkipsHeldHandles(t *testing.T) {
	const ttl = time.Minute
	m := testManager(t, ttl)

	h, err := m.Acquire("busy")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m.sweep(time.Now().Add(ttl * 10))
	if s := statFor(t, m, "busy"); !s.Open {
		t.Fatal("swept a DB with an outstanding handle")
	}
	if err := h.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestManagerReaperGoroutine(t *testing.T) {
	m, err := NewManager(ManagerOptions{
		BaseDir:       t.TempDir(),
		IdleTTL:       20 * time.Millisecond,
		SweepInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	h, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	h.Release()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !statFor(t, m, "logs").Open {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("reaper never closed the idle DB")
}

func TestManagerDoubleReleaseIsSafe(t *testing.T) {
	m := testManager(t, 0)

	h1, err := m.Acquire("x")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := m.Acquire("x")
	if err != nil {
		t.Fatal(err)
	}
	h1.Release()
	h1.Release()
	h1.Close()
	if s := statFor(t, m, "x"); s.Refs != 1 {
		t.Fatalf("refs = %d after double release, want 1", s.Refs)
	}
	h2.Release()
	if s := statFor(t, m, "x"); s.Refs != 0 {
		t.Fatalf("refs = %d, want 0", s.Refs)
	}
}

func TestManagerClose(t *testing.T) {
	m := testManager(t, 0)

	h, err := m.Acquire("logs")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire("logs"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}
	if _, err := h.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on outstanding handle, got %v", err)
	}
	if err := m.Close(); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("second Close: expected ErrManagerClosed, got %v", err)
	}
}

func TestManagerNameValidation(t *testing.T) {
	m := testManager(t, 0)
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b"} {
		if _, err := m.Acquire(name); err == nil {
			t.Errorf("Acquire(%q) succeeded, want error", name)
		}
	}
}

func TestManagerDBOptionsDirIsForced(t *testing.T) {
	base := t.TempDir()
	rogue := t.TempDir()
	m, err := NewManager(ManagerOptions{
		BaseDir: base,
		DBOptions: func(_, _ string) Options {
			return DefaultOptions(rogue) // must be overridden with the assigned dir
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	h, err := m.Acquire("a")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Observable proof instead of reaching into DB's unexported Options:
	// segment files must land under the assigned directory, not the
	// callback's rogue one.
	assigned, err := os.ReadDir(filepath.Join(base, "a"))
	if err != nil || len(assigned) == 0 {
		t.Fatalf("expected segment files under the assigned dir: entries=%v err=%v", assigned, err)
	}
	rogueEntries, err := os.ReadDir(rogue)
	if err != nil {
		t.Fatal(err)
	}
	if len(rogueEntries) != 0 {
		t.Fatal("DBOptions callback overrode the assigned directory")
	}
}

func TestManagerConcurrentAcquireRelease(t *testing.T) {
	m := testManager(t, time.Minute)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			name := fmt.Sprintf("db-%d", g%3)
			for i := range 50 {
				h, err := m.Acquire(name)
				if err != nil {
					t.Error(err)
					return
				}
				key := fmt.Appendf(nil, "g%d-i%d", g, i)
				if err := h.Put(key, []byte("v")); err != nil {
					t.Error(err)
					h.Release()
					return
				}
				h.Release()
				if i%10 == 0 {
					m.sweep(time.Now().Add(2 * time.Minute))
				}
			}
		}(g)
	}
	wg.Wait()

	// Everything written must still be readable after all the churn.
	for g := range 8 {
		h, err := m.Acquire(fmt.Sprintf("db-%d", g%3))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 50 {
			key := fmt.Appendf(nil, "g%d-i%d", g, i)
			if _, err := h.Get(key); err != nil {
				t.Fatalf("Get(%s): %v", key, err)
			}
		}
		h.Release()
	}
}
