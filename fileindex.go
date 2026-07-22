package kv

import "github.com/Gandalfalex/in_memory_db/internal/fileindex"

// FileIndex is the plain-JSONL sibling of DB — see its doc comment.
// Implementation lives in internal/fileindex.
type FileIndex = fileindex.FileIndex

// KeyFunc extracts the index key from one line of a FileIndex's file (or
// a SortedIndex source file). See internal/kvtypes.KeyFunc.
type KeyFunc = fileindex.KeyFunc

// OpenFileIndex opens (creating if necessary) the line file at path and
// rebuilds the in-memory index from it with one sequential scan.
func OpenFileIndex(path string, keyFunc KeyFunc) (*FileIndex, error) {
	return fileindex.OpenFileIndex(path, keyFunc)
}

// JSONStringKey returns a KeyFunc that extracts the named top-level
// string field from a JSON object line without unmarshalling the whole
// object.
func JSONStringKey(field string) KeyFunc { return fileindex.JSONStringKey(field) }
