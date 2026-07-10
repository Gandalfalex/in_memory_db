package kv

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"sync"
)

// KeyFunc extracts the index key from one line of a FileIndex's file,
// without necessarily unmarshalling the whole line (for JSONL, see
// JSONStringKey). Returning ok=false skips the line: that is how blank,
// malformed, and torn-tail lines are excluded during the rebuild scan, so
// the file needs no CRC framing. It must be deterministic — Put derives
// the key it indexes under by calling it on the new line, and reopen must
// derive the same key from the same bytes.
type KeyFunc func(line []byte) (key []byte, ok bool)

// lineLoc locates one line's payload within the file: byte offset and
// length, excluding the trailing newline.
type lineLoc struct {
	off int64
	len int
}

// FileIndex is the plain-file sibling of DB: the source of truth is a
// human/tool-readable append-only line file (typically JSONL) holding
// exactly the caller's bytes — no kv-owned envelope, segments, snapshots,
// or compaction — plus a pure-RAM index (key → offset/length) rebuilt on
// every Open by one sequential scan, last line wins per key.
//
// Put appends a line and repoints the index; superseded lines stay on
// disk untouched (dead but never lost). The file only grows: with one
// run's data bounding its size, recovery stays cheap by construction and
// nothing ever needs reclaiming.
type FileIndex struct {
	path    string
	keyFunc KeyFunc

	mu           sync.RWMutex
	f            *os.File
	size         int64 // append offset: current file length
	unterminated bool  // file is non-empty and doesn't end in '\n'
	idx          map[string]lineLoc
	closed       bool
}

// OpenFileIndex opens (creating if necessary) the line file at path and
// rebuilds the in-memory index from it with one sequential scan, calling
// keyFunc on every line; for duplicate keys the last line wins. Lines
// keyFunc rejects (blank, malformed, torn tail) are skipped, not errors.
func OpenFileIndex(path string, keyFunc KeyFunc) (*FileIndex, error) {
	if path == "" {
		return nil, fmt.Errorf("kv: file index path is required")
	}
	if keyFunc == nil {
		return nil, fmt.Errorf("kv: file index keyFunc is required")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kv: open file index: %w", err)
	}
	fi := &FileIndex{path: path, keyFunc: keyFunc, f: f, idx: make(map[string]lineLoc)}
	if err := fi.rebuild(); err != nil {
		f.Close()
		return nil, err
	}
	return fi, nil
}

// rebuild scans the whole file once, indexing every line keyFunc accepts.
func (fi *FileIndex) rebuild() error {
	r := bufio.NewReaderSize(fi.f, 64<<10)
	var off int64
	for {
		chunk, err := r.ReadBytes('\n')
		line := chunk
		terminated := false
		if n := len(chunk); n > 0 && chunk[n-1] == '\n' {
			line = chunk[:n-1]
			terminated = true
		}
		if len(line) > 0 {
			if key, ok := fi.keyFunc(line); ok && len(key) > 0 {
				fi.idx[string(key)] = lineLoc{off: off, len: len(line)}
			}
		}
		off += int64(len(chunk))
		if err != nil {
			if errors.Is(err, io.EOF) {
				fi.size = off
				fi.unterminated = len(chunk) > 0 && !terminated
				return nil
			}
			return fmt.Errorf("kv: scan file index: %w", err)
		}
	}
}

// Path returns the file the index is backed by.
func (fi *FileIndex) Path() string { return fi.path }

// Put appends line (plus a trailing newline) to the file and points the
// index at it, superseding any previous line for the same key. The key is
// derived by calling the KeyFunc on line — the same derivation the rebuild
// scan uses — so a line the KeyFunc rejects is refused with ErrNoLineKey
// rather than written unrecoverably. The line must not itself contain a
// newline. Durability is buffered; call Sync for an explicit fsync.
func (fi *FileIndex) Put(line []byte) error {
	key, ok := fi.keyFunc(line)
	if !ok || len(key) == 0 {
		return ErrNoLineKey
	}
	if bytes.IndexByte(line, '\n') >= 0 {
		return ErrInvalidLine
	}

	fi.mu.Lock()
	defer fi.mu.Unlock()
	if fi.closed {
		return ErrClosed
	}

	buf := make([]byte, 0, len(line)+2)
	if fi.unterminated {
		// Seal a torn/newline-less tail first so this line can't merge
		// into it.
		buf = append(buf, '\n')
	}
	lineOff := fi.size + int64(len(buf))
	buf = append(buf, line...)
	buf = append(buf, '\n')

	n, err := fi.f.WriteAt(buf, fi.size)
	if n > 0 {
		fi.size += int64(n)
		fi.unterminated = buf[n-1] != '\n'
	}
	if err != nil {
		// A partial write leaves a torn tail; unterminated is already set
		// so the next Put seals it, and the rebuild scan skips it.
		return fmt.Errorf("kv: append to file index: %w", err)
	}
	fi.idx[string(key)] = lineLoc{off: lineOff, len: len(line)}
	return nil
}

// Get returns a copy of key's current line (without the trailing
// newline), or ErrNotFound.
func (fi *FileIndex) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	if fi.closed {
		return nil, ErrClosed
	}
	loc, ok := fi.idx[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, loc.len)
	if _, err := fi.f.ReadAt(out, loc.off); err != nil {
		return nil, fmt.Errorf("kv: read file index line: %w", err)
	}
	return out, nil
}

// Has reports whether key currently has an indexed line.
func (fi *FileIndex) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	if fi.closed {
		return false, ErrClosed
	}
	_, ok := fi.idx[string(key)]
	return ok, nil
}

// Delete removes key from the in-memory index only; its lines stay on
// disk (the file is append-only and carries no kv-owned envelope, so
// there is no tombstone to write). That means a Delete does NOT survive
// reopen: the rebuild scan re-indexes the orphaned line and the key
// reappears. For durable removal, Put a line your KeyFunc maps to the
// same key that your readers treat as a deletion marker. Deleting an
// absent key is not an error.
func (fi *FileIndex) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if fi.closed {
		return ErrClosed
	}
	delete(fi.idx, string(key))
	return nil
}

// Len returns the number of currently indexed keys.
func (fi *FileIndex) Len() int {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return len(fi.idx)
}

// Sync fsyncs the file, making every previously appended line durable.
func (fi *FileIndex) Sync() error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if fi.closed {
		return ErrClosed
	}
	return fi.f.Sync()
}

// Close syncs and closes the file. Safe to call once; a second call
// returns ErrClosed.
func (fi *FileIndex) Close() error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if fi.closed {
		return ErrClosed
	}
	fi.closed = true
	if err := fi.f.Sync(); err != nil {
		fi.f.Close()
		return fmt.Errorf("kv: sync file index: %w", err)
	}
	if err := fi.f.Close(); err != nil {
		return fmt.Errorf("kv: close file index: %w", err)
	}
	return nil
}

// Iterator returns a new Iterator over keys matching opts, with the same
// snapshot-of-keys/lazy-values behavior as DB.Iterator.
func (fi *FileIndex) Iterator(opts IterOptions) *Iterator {
	fi.mu.RLock()
	keys := make([][]byte, 0, len(fi.idx))
	for k := range fi.idx {
		if len(opts.Prefix) > 0 && !strings.HasPrefix(k, string(opts.Prefix)) {
			continue
		}
		keys = append(keys, []byte(k))
	}
	fi.mu.RUnlock()

	return newIterator(keys, opts, fi.Get)
}

// All returns an iterator over key/line pairs matching opts, for use with
// range-over-func. A read error silently ends the iteration early; use
// Iterator directly when errors must be distinguished from exhaustion.
func (fi *FileIndex) All(opts IterOptions) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := fi.Iterator(opts)
		defer it.Close()
		for it.Next() {
			if !yield(it.Key(), it.Value()) {
				return
			}
		}
	}
}

// JSONStringKey returns a KeyFunc that extracts the named top-level
// string field from a JSON object line without unmarshalling the whole
// object: it validates the line is well-formed JSON (this is what rejects
// torn tails, which can otherwise still yield the key field before the
// truncation point), then token-scans the top level and stops at the
// first match. Lines that aren't valid JSON objects, lack the field, or
// hold a non-string/empty value are skipped (ok=false).
func JSONStringKey(field string) KeyFunc {
	return func(line []byte) ([]byte, bool) {
		if !json.Valid(line) {
			return nil, false
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
			return nil, false
		}
		for dec.More() {
			nameTok, err := dec.Token()
			if err != nil {
				return nil, false
			}
			name, _ := nameTok.(string)
			if name == field {
				valTok, err := dec.Token()
				if err != nil {
					return nil, false
				}
				s, ok := valTok.(string)
				if !ok || s == "" {
					return nil, false
				}
				return []byte(s), true
			}
			if err := skipJSONValue(dec); err != nil {
				return nil, false
			}
		}
		return nil, false
	}
}

// skipJSONValue consumes exactly one JSON value (scalar, object, or
// array) from dec.
func skipJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok || (d != '{' && d != '[') {
		return nil
	}
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}
