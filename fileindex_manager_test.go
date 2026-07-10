package kv

import (
	"errors"
	"testing"
	"time"
)

func testFileIndexManager(t *testing.T, ttl time.Duration) *FileIndexManager {
	t.Helper()
	m, err := NewFileIndexManager(FileIndexManagerOptions{
		BaseDir: t.TempDir(),
		KeyFunc: JSONStringKey("id"),
		IdleTTL: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func fileIndexStatFor(t *testing.T, m *FileIndexManager, name string) ManagedDBStat {
	t.Helper()
	for _, s := range m.Stats() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no stats entry for %q", name)
	return ManagedDBStat{}
}

func TestFileIndexManagerRoundtrip(t *testing.T) {
	m := testFileIndexManager(t, 0)

	h, err := m.Acquire("traces.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if h.Name() != "traces.jsonl" {
		t.Fatalf("Name() = %q", h.Name())
	}
	if err := h.Put([]byte(`{"id":"a","v":1}`)); err != nil {
		t.Fatal(err)
	}
	h.Release()

	h2, err := m.Acquire("traces.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	got, err := h2.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","v":1}` {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if h.FileIndex != h2.FileIndex {
		t.Fatal("expected both handles to share one *FileIndex while cached")
	}
}

func TestFileIndexManagerSweepClosesIdleAndReopens(t *testing.T) {
	const ttl = time.Minute
	m := testFileIndexManager(t, ttl)

	h, err := m.Acquire("logs.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Put([]byte(`{"id":"k","v":"survives reap"}`)); err != nil {
		t.Fatal(err)
	}
	h.Release()

	m.sweep(time.Now())
	if s := fileIndexStatFor(t, m, "logs.jsonl"); !s.Open {
		t.Fatal("swept before IdleTTL elapsed")
	}

	m.sweep(time.Now().Add(ttl + time.Second))
	s := fileIndexStatFor(t, m, "logs.jsonl")
	if s.Open {
		t.Fatal("expected idle file index to be closed")
	}
	if s.LastReapErr != nil {
		t.Fatalf("reap close failed: %v", s.LastReapErr)
	}

	// Stale handle now reports closed.
	if _, err := h.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed via stale handle, got %v", err)
	}

	// Reacquire rebuilds from the file with data intact.
	h2, err := m.Acquire("logs.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	got, err := h2.Get([]byte("k"))
	if err != nil || string(got) != `{"id":"k","v":"survives reap"}` {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}

func TestFileIndexManagerSweepSkipsHeldHandles(t *testing.T) {
	const ttl = time.Minute
	m := testFileIndexManager(t, ttl)

	h, err := m.Acquire("busy.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m.sweep(time.Now().Add(ttl * 10))
	if s := fileIndexStatFor(t, m, "busy.jsonl"); !s.Open {
		t.Fatal("swept a file index with an outstanding handle")
	}
	if err := h.Put([]byte(`{"id":"k"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestFileIndexManagerClose(t *testing.T) {
	m := testFileIndexManager(t, 0)

	h, err := m.Acquire("logs.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Put([]byte(`{"id":"k"}`)); err != nil {
		t.Fatal(err)
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire("logs.jsonl"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}
	if _, err := h.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on outstanding handle, got %v", err)
	}
	if err := m.Close(); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("second Close: expected ErrManagerClosed, got %v", err)
	}
}

func TestFileIndexManagerNameValidation(t *testing.T) {
	m := testFileIndexManager(t, 0)
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b"} {
		if _, err := m.Acquire(name); err == nil {
			t.Errorf("Acquire(%q) succeeded, want error", name)
		}
	}
}

func TestFileIndexManagerRequiredOptions(t *testing.T) {
	if _, err := NewFileIndexManager(FileIndexManagerOptions{KeyFunc: JSONStringKey("id")}); err == nil {
		t.Error("missing BaseDir accepted")
	}
	if _, err := NewFileIndexManager(FileIndexManagerOptions{BaseDir: t.TempDir()}); err == nil {
		t.Error("missing KeyFunc accepted")
	}
}
