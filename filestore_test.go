package kv

import (
	"errors"
	"path/filepath"
	"testing"
)

func openTestFileIndexStore(t *testing.T) *FileIndexStore {
	t.Helper()
	fi, err := OpenFileIndex(filepath.Join(t.TempDir(), "data.jsonl"), JSONStringKey("id"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fi.Close() })
	return NewFileIndexStore(fi, JSONLineCodec{})
}

func TestFileIndexStoreRoundtrip(t *testing.T) {
	s := openTestFileIndexStore(t)

	if err := s.Put([]byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get([]byte("a"))
	if err != nil || string(got) != `{"n":1}` {
		t.Fatalf("Get(a) = %q, %v", got, err)
	}
	if has, err := s.Has([]byte("a")); err != nil || !has {
		t.Fatalf("Has(a) = %v, %v", has, err)
	}
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(a) after delete = %v, want ErrNotFound", err)
	}
}

// The line FileIndexStore writes must still be a valid, readable line for
// the underlying FileIndex's own KeyFunc — this pins that the two stay in
// sync (JSONLineCodec's default field pairs with JSONStringKey's default).
func TestFileIndexStoreLineIsReadableByFileIndexDirectly(t *testing.T) {
	fi, err := OpenFileIndex(filepath.Join(t.TempDir(), "data.jsonl"), JSONStringKey("id"))
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	s := NewFileIndexStore(fi, JSONLineCodec{})

	if err := s.Put([]byte("k1"), []byte(`{"n":42}`)); err != nil {
		t.Fatal(err)
	}
	line, err := fi.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("fi.Get(k1): %v", err)
	}
	if string(line) != `{"id":"k1","value":{"n":42}}` {
		t.Fatalf("line = %q", line)
	}
}

func TestFileIndexStoreCustomKeyField(t *testing.T) {
	fi, err := OpenFileIndex(filepath.Join(t.TempDir(), "data.jsonl"), JSONStringKey("uuid"))
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	s := NewFileIndexStore(fi, JSONLineCodec{KeyField: "uuid"})

	if err := s.Put([]byte("k1"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get([]byte("k1"))
	if err != nil || string(got) != `{"n":1}` {
		t.Fatalf("Get(k1) = %q, %v", got, err)
	}
}

// A FileIndexStore whose LineCodec builds a different field than the
// underlying FileIndex's own KeyFunc expects must fail loudly on Put
// (ErrNoLineKey, the same error FileIndex.Put itself returns when its
// KeyFunc can't find a key) — not silently write a line that can never be
// read back by key.
func TestFileIndexStoreKeyFieldMismatchFailsLoudly(t *testing.T) {
	fi, err := OpenFileIndex(filepath.Join(t.TempDir(), "data.jsonl"), JSONStringKey("id")) // expects "id"
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	s := NewFileIndexStore(fi, JSONLineCodec{KeyField: "uuid"}) // builds "uuid" instead

	err = s.Put([]byte("k1"), []byte(`{"n":1}`))
	if !errors.Is(err, ErrNoLineKey) {
		t.Fatalf("Put with mismatched KeyField = %v, want ErrNoLineKey", err)
	}
	if fi.Len() != 0 {
		t.Fatalf("Len() = %d, want 0 (rejected line must not be indexed)", fi.Len())
	}
}

func TestJSONRawFieldInvalidInput(t *testing.T) {
	if _, ok := jsonRawField([]byte("not json"), "value"); ok {
		t.Fatal("expected ok=false for invalid JSON")
	}
	if _, ok := jsonRawField([]byte(`{"other":1}`), "value"); ok {
		t.Fatal("expected ok=false when the field is absent")
	}
}

// FuzzJSONRawField: jsonRawField backs FileIndexStore.Get for every
// stored line, so a malformed or adversarial line (this package doesn't
// control what's on disk — see FileIndex's whole design) must yield
// ok=false, never a panic.
func FuzzJSONRawField(f *testing.F) {
	f.Add([]byte(`{"value":{"n":1}}`), "value")
	f.Add([]byte(`{"value":[1,2,3]}`), "value")
	f.Add([]byte(`{"value":"a string"}`), "value")
	f.Add([]byte(`{"a":1}`), "b")
	f.Add([]byte(`not json`), "value")
	f.Add([]byte(`{`), "value")
	f.Add([]byte(""), "")

	f.Fuzz(func(t *testing.T, line []byte, field string) {
		raw, ok := jsonRawField(line, field)
		if ok && len(raw) == 0 {
			t.Fatalf("ok=true but empty raw value for line %q field %q", line, field)
		}
	})
}

// LineCodec.Value must return whatever JSON value was spliced in by
// Build verbatim, not just JSON objects — a Codec can just as validly
// encode a value as a JSON array, string, number, bool, or null.
func TestJSONLineCodecNonObjectValues(t *testing.T) {
	c := JSONLineCodec{}
	for _, value := range [][]byte{
		[]byte(`[1,2,3]`),
		[]byte(`"a string"`),
		[]byte(`42`),
		[]byte(`true`),
		[]byte(`null`),
	} {
		line := c.Build([]byte("k"), value)
		got, ok := c.Value(line)
		if !ok {
			t.Fatalf("Value() failed to extract from line %q", line)
		}
		if string(got) != string(value) {
			t.Fatalf("Value() = %q, want %q", got, value)
		}
	}
}

// --- Bucket over FileIndexStore --------------------------------------------

func TestBucketOverFileIndexStoreRoundtrip(t *testing.T) {
	s := openTestFileIndexStore(t)
	users := NewBucket[user](s, "users:", JSONCodec[user]{})

	if err := users.Put("alice", user{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	got, err := users.Get("alice")
	if err != nil || got.Name != "Alice" || got.Age != 30 {
		t.Fatalf("Get(alice) = %+v, %v", got, err)
	}
	if has, err := users.Has("alice"); err != nil || !has {
		t.Fatalf("Has(alice) = %v, %v", has, err)
	}
	if err := users.Delete("alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Get("alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(alice) after delete = %v, want ErrNotFound", err)
	}
}

func TestBucketOverFileIndexStoreIterator(t *testing.T) {
	s := openTestFileIndexStore(t)
	users := NewBucket[user](s, "users:", JSONCodec[user]{})
	must(t, users.Put("alice", user{Name: "Alice", Age: 30}))
	must(t, users.Put("bob", user{Name: "Bob", Age: 25}))

	got := map[string]int{}
	for name, u := range users.All("") {
		got[name] = u.Age
	}
	if len(got) != 2 || got["alice"] != 30 || got["bob"] != 25 {
		t.Fatalf("All() = %v", got)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
