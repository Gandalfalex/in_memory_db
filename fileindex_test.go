package kv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// csvKey indexes "key,rest..." lines: everything before the first comma.
func csvKey(line []byte) ([]byte, bool) {
	i := bytes.IndexByte(line, ',')
	if i <= 0 {
		return nil, false
	}
	return line[:i], true
}

func openTestFileIndex(t *testing.T, keyFunc KeyFunc) (*FileIndex, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.jsonl")
	fi, err := OpenFileIndex(path, keyFunc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fi.Close() })
	return fi, path
}

func TestFileIndexPutGetRoundtrip(t *testing.T) {
	fi, _ := openTestFileIndex(t, csvKey)

	if err := fi.Put([]byte("a,1")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Put([]byte("b,2")); err != nil {
		t.Fatal(err)
	}
	got, err := fi.Get([]byte("a"))
	if err != nil || string(got) != "a,1" {
		t.Fatalf("Get(a) = %q, %v", got, err)
	}
	if ok, err := fi.Has([]byte("b")); err != nil || !ok {
		t.Fatalf("Has(b) = %v, %v", ok, err)
	}
	if _, err := fi.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(c) = %v, want ErrNotFound", err)
	}
	if fi.Len() != 2 {
		t.Fatalf("Len = %d, want 2", fi.Len())
	}
}

func TestFileIndexLastLineWins(t *testing.T) {
	fi, path := openTestFileIndex(t, csvKey)

	for i := 0; i < 3; i++ {
		if err := fi.Put(fmt.Appendf(nil, "k,v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := fi.Get([]byte("k"))
	if err != nil || string(got) != "k,v2" {
		t.Fatalf("Get = %q, %v", got, err)
	}

	// Old lines stay on disk untouched: exactly the caller's bytes, in
	// order, newline-terminated, no envelope.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "k,v0\nk,v1\nk,v2\n" {
		t.Fatalf("file = %q", raw)
	}
}

func TestFileIndexReopenRebuilds(t *testing.T) {
	fi, path := openTestFileIndex(t, csvKey)
	if err := fi.Put([]byte("a,1")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Put([]byte("b,2")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Put([]byte("a,updated")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}

	fi2, err := OpenFileIndex(path, csvKey)
	if err != nil {
		t.Fatal(err)
	}
	defer fi2.Close()
	got, err := fi2.Get([]byte("a"))
	if err != nil || string(got) != "a,updated" {
		t.Fatalf("Get(a) after reopen = %q, %v", got, err)
	}
	if fi2.Len() != 2 {
		t.Fatalf("Len after reopen = %d, want 2", fi2.Len())
	}
}

func TestFileIndexDeleteIsIndexOnly(t *testing.T) {
	fi, path := openTestFileIndex(t, csvKey)
	if err := fi.Put([]byte("a,1")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := fi.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
	// Deleting an absent key is not an error.
	if err := fi.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}

	// Documented semantics: the line is still on disk, so the key
	// reappears after reopen.
	fi2, err := OpenFileIndex(path, csvKey)
	if err != nil {
		t.Fatal(err)
	}
	defer fi2.Close()
	if ok, _ := fi2.Has([]byte("a")); !ok {
		t.Fatal("index-only delete unexpectedly survived reopen")
	}
}

func TestFileIndexSkipsBlankAndMalformedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.txt")
	content := "a,1\n\nnocomma\n,leadingcomma\nb,2\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := OpenFileIndex(path, csvKey)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	if fi.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (blank/malformed skipped)", fi.Len())
	}
}

func TestFileIndexTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	keyFunc := JSONStringKey("id")
	// A good line followed by a torn (truncated, unterminated) one.
	if err := os.WriteFile(path, []byte(`{"id":"a","v":1}`+"\n"+`{"id":"b","v`), 0o644); err != nil {
		t.Fatal(err)
	}

	fi, err := OpenFileIndex(path, keyFunc)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (torn tail skipped)", fi.Len())
	}
	if ok, _ := fi.Has([]byte("b")); ok {
		t.Fatal("torn line was indexed")
	}

	// The next Put must seal the torn tail so the new line can't merge
	// into it.
	if err := fi.Put([]byte(`{"id":"c","v":3}`)); err != nil {
		t.Fatal(err)
	}
	got, err := fi.Get([]byte("c"))
	if err != nil || string(got) != `{"id":"c","v":3}` {
		t.Fatalf("Get(c) = %q, %v", got, err)
	}
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: still exactly a and c.
	fi2, err := OpenFileIndex(path, keyFunc)
	if err != nil {
		t.Fatal(err)
	}
	defer fi2.Close()
	if fi2.Len() != 2 {
		t.Fatalf("Len after reopen = %d, want 2", fi2.Len())
	}
	if got, err := fi2.Get([]byte("c")); err != nil || string(got) != `{"id":"c","v":3}` {
		t.Fatalf("Get(c) after reopen = %q, %v", got, err)
	}
}

func TestFileIndexNewlinelessTailIsUsable(t *testing.T) {
	// A hand-written file whose last line is valid but lacks the final
	// newline: it must be indexed, and a Put must not merge into it.
	path := filepath.Join(t.TempDir(), "data.txt")
	if err := os.WriteFile(path, []byte("a,1\nb,2"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := OpenFileIndex(path, csvKey)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	if got, err := fi.Get([]byte("b")); err != nil || string(got) != "b,2" {
		t.Fatalf("Get(b) = %q, %v", got, err)
	}
	if err := fi.Put([]byte("c,3")); err != nil {
		t.Fatal(err)
	}
	if got, err := fi.Get([]byte("b")); err != nil || string(got) != "b,2" {
		t.Fatalf("Get(b) after Put = %q, %v", got, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "a,1\nb,2\nc,3\n" {
		t.Fatalf("file = %q", raw)
	}
}

func TestFileIndexPutValidation(t *testing.T) {
	fi, _ := openTestFileIndex(t, csvKey)

	if err := fi.Put([]byte("nokeyhere")); !errors.Is(err, ErrNoLineKey) {
		t.Fatalf("Put(no key) = %v, want ErrNoLineKey", err)
	}
	if err := fi.Put([]byte("a,em\nbedded")); !errors.Is(err, ErrInvalidLine) {
		t.Fatalf("Put(embedded newline) = %v, want ErrInvalidLine", err)
	}
	if fi.Len() != 0 {
		t.Fatalf("Len = %d after rejected Puts, want 0", fi.Len())
	}
}

func TestFileIndexClosed(t *testing.T) {
	fi, _ := openTestFileIndex(t, csvKey)
	if err := fi.Put([]byte("a,1")); err != nil {
		t.Fatal(err)
	}
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fi.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close = %v, want ErrClosed", err)
	}
	if _, err := fi.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if err := fi.Put([]byte("b,2")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close = %v, want ErrClosed", err)
	}
	if err := fi.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close = %v, want ErrClosed", err)
	}
}

func TestFileIndexIterator(t *testing.T) {
	fi, _ := openTestFileIndex(t, csvKey)
	for _, line := range []string{"b,2", "a,1", "ab,3", "c,4"} {
		if err := fi.Put([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}

	var keys []string
	for k, v := range fi.All(IterOptions{Sorted: true}) {
		keys = append(keys, string(k))
		if want := string(k) + ","; string(v)[:len(want)] != want {
			t.Fatalf("value %q doesn't match key %q", v, k)
		}
	}
	if got := fmt.Sprint(keys); got != "[a ab b c]" {
		t.Fatalf("sorted keys = %v", keys)
	}

	keys = keys[:0]
	for k := range fi.All(IterOptions{Prefix: []byte("a"), Sorted: true, Reverse: true}) {
		keys = append(keys, string(k))
	}
	if got := fmt.Sprint(keys); got != "[ab a]" {
		t.Fatalf("prefix+sorted+reverse keys = %v", keys)
	}

	// Keys deleted after iterator creation are skipped, mirroring
	// DB.Iterator.
	it := fi.Iterator(IterOptions{Sorted: true})
	defer it.Close()
	if err := fi.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	keys = keys[:0]
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(keys); got != "[ab b c]" {
		t.Fatalf("keys after mid-iteration delete = %v", keys)
	}
}

func TestJSONStringKey(t *testing.T) {
	keyFunc := JSONStringKey("id")
	cases := []struct {
		line string
		key  string
		ok   bool
	}{
		{`{"id":"a"}`, "a", true},
		{`{"v":1,"id":"later"}`, "later", true},
		{`{"nested":{"id":"inner"},"id":"outer"}`, "outer", true},
		{`{"arr":[{"id":"x"},2,[3]],"id":"after-arr"}`, "after-arr", true},
		{`{"id":"a","extra":"ignored"}`, "a", true},
		{`{"v":1}`, "", false},          // field missing
		{`{"id":42}`, "", false},        // non-string value
		{`{"id":""}`, "", false},        // empty key useless
		{`{"id":"a"`, "", false},        // torn: invalid JSON
		{`["id","a"]`, "", false},       // not an object
		{`"id"`, "", false},             // scalar
		{`not json at all`, "", false},  // garbage
		{``, "", false},                 // blank
		{`{"outer":{"id":"x"}}`, "", false}, // only nested, not top-level
	}
	for _, c := range cases {
		key, ok := keyFunc([]byte(c.line))
		if ok != c.ok || string(key) != c.key {
			t.Errorf("JSONStringKey(%q) = %q, %v; want %q, %v", c.line, key, ok, c.key, c.ok)
		}
	}
}

func TestFileIndexReopenAfterSyncSurvives(t *testing.T) {
	fi, path := openTestFileIndex(t, JSONStringKey("id"))
	if err := fi.Put([]byte(`{"id":"a","payload":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := fi.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}
	fi2, err := OpenFileIndex(path, JSONStringKey("id"))
	if err != nil {
		t.Fatal(err)
	}
	defer fi2.Close()
	got, err := fi2.Get([]byte("a"))
	if err != nil || string(got) != `{"id":"a","payload":"x"}` {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}
