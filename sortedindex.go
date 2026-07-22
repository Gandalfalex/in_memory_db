package kv

import (
	"context"
	"iter"

	"github.com/Gandalfalex/in_memory_db/internal/sortedindex"
)

// SortedIndex is a read-only, lexicographically-sorted directory over one
// or more FileIndex-style source files — see its doc comment.
// Implementation lives in internal/sortedindex.
type SortedIndex = sortedindex.SortedIndex

// SortedIndexOptions tunes the RAM/lookup-cost tradeoff of a build.
type SortedIndexOptions = sortedindex.SortedIndexOptions

// BuildSortedIndex scans sourcePaths once each, in precedence order, and
// writes a lexicographically sorted directory to sidxPath via external
// merge sort. See internal/sortedindex.BuildSortedIndex for the full
// contract (concurrency, cancellation, precedence rules).
func BuildSortedIndex(ctx context.Context, sourcePaths []string, keyFunc KeyFunc, sidxPath string, opts SortedIndexOptions) error {
	return sortedindex.BuildSortedIndex(ctx, sourcePaths, keyFunc, sidxPath, opts)
}

// OpenSortedIndex opens a directory previously written by BuildSortedIndex.
func OpenSortedIndex(sidxPath string, keyFunc KeyFunc) (*SortedIndex, error) {
	return sortedindex.OpenSortedIndex(sidxPath, keyFunc)
}

// EnsureFresh returns an open SortedIndex over sourcePaths, rebuilding or
// incrementally refreshing sidxPath first only if needed. See
// internal/sortedindex.EnsureFresh for the full contract.
func EnsureFresh(ctx context.Context, sourcePaths []string, sidxPath string, keyFunc KeyFunc, opts SortedIndexOptions) (*SortedIndex, error) {
	return sortedindex.EnsureFresh(ctx, sourcePaths, sidxPath, keyFunc, opts)
}

// FilterAll wraps a (key, line) iterator with a caller predicate over the
// raw line bytes.
func FilterAll(seq iter.Seq2[[]byte, []byte], keep func(line []byte) bool) iter.Seq2[[]byte, []byte] {
	return sortedindex.FilterAll(seq, keep)
}
