package kv

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// LineCodec bridges FileIndex's one-self-describing-line write model onto
// Store's explicit Put(key, value)/Get(key)->value contract: Build
// combines a key and an already-Codec-encoded value into the line to
// append, and Value extracts the value bytes back out of a stored line.
// Build's output must be a line the owning FileIndex's own KeyFunc
// extracts key back out of — that pairing is the caller's responsibility
// (same as it already is between KeyFunc and whatever writes lines
// FileIndex.Put directly), and a mismatch writes entries that can never
// be read back by key. JSONLineCodec is the built-in implementation,
// pairing with JSONStringKey.
type LineCodec interface {
	Build(key, value []byte) (line []byte)
	Value(line []byte) (value []byte, ok bool)
}

// JSONLineCodec builds and parses lines shaped
// {"<KeyField>":"<key>","value":<value>}, pairing with
// JSONStringKey(KeyField) as the FileIndex's KeyFunc. value must already
// be valid JSON (e.g. produced by JSONCodec) — it is spliced in raw, not
// re-encoded or escaped. KeyField defaults to "id" when empty.
type JSONLineCodec struct{ KeyField string }

func (c JSONLineCodec) keyField() string {
	if c.KeyField == "" {
		return "id"
	}
	return c.KeyField
}

func (c JSONLineCodec) Build(key, value []byte) []byte {
	fieldJSON, _ := json.Marshal(c.keyField())
	keyJSON, _ := json.Marshal(string(key))
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.Write(fieldJSON)
	buf.WriteByte(':')
	buf.Write(keyJSON)
	buf.WriteString(`,"value":`)
	buf.Write(value)
	buf.WriteByte('}')
	return buf.Bytes()
}

func (c JSONLineCodec) Value(line []byte) ([]byte, bool) {
	return jsonRawField(line, "value")
}

// jsonRawField returns the raw (still-encoded) JSON bytes of line's
// top-level field, without decoding them into a Go value — the value
// might be an object, array, or scalar, and the caller (a Codec) is what
// decodes it, not this lookup.
func jsonRawField(line []byte, field string) ([]byte, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return nil, false
	}
	v, ok := m[field]
	return []byte(v), ok
}

// FileIndexStore adapts *FileIndex to the Store interface via a
// LineCodec, so Bucket (or any Store-generic code) can wrap a FileIndex
// the same way it wraps a DB. See LineCodec's doc comment for the
// key/line pairing responsibility this rests on.
type FileIndexStore struct {
	fi    *FileIndex
	codec LineCodec
}

// NewFileIndexStore returns a Store view of fi, using codec to translate
// between Store's (key, value) shape and fi's self-describing lines.
func NewFileIndexStore(fi *FileIndex, codec LineCodec) *FileIndexStore {
	return &FileIndexStore{fi: fi, codec: codec}
}

func (s *FileIndexStore) Get(key []byte) ([]byte, error) {
	line, err := s.fi.Get(key)
	if err != nil {
		return nil, err
	}
	value, ok := s.codec.Value(line)
	if !ok {
		return nil, fmt.Errorf("kv: line for key %q has no value field", key)
	}
	return value, nil
}

func (s *FileIndexStore) Has(key []byte) (bool, error) { return s.fi.Has(key) }

func (s *FileIndexStore) Put(key, value []byte) error {
	return s.fi.Put(s.codec.Build(key, value))
}

func (s *FileIndexStore) Delete(key []byte) error { return s.fi.Delete(key) }

func (s *FileIndexStore) Iterator(opts IterOptions) *Iterator { return s.fi.Iterator(opts) }

// DecodeValue extracts the value portion out of a raw line, as returned
// by Iterator's Value() — see Store.DecodeValue.
func (s *FileIndexStore) DecodeValue(raw []byte) ([]byte, bool) { return s.codec.Value(raw) }
