package kv

import "github.com/Gandalfalex/in_memory_db/internal/bitcask"

// DB is an embedded, single-process key-value store: an append-only,
// memory-mapped segment log on disk plus a compact in-memory index. See
// README for the full design rationale; the implementation lives in
// internal/bitcask.
type DB = bitcask.DB

// Options configures a DB.
type Options = bitcask.Options

// Stats is a point-in-time snapshot of a DB's size counters.
type Stats = bitcask.Stats

// MaxKeyLen is the largest key size Put accepts, in bytes.
const MaxKeyLen = bitcask.MaxKeyLen

// DefaultSegmentSize is the rotation threshold for a new active segment
// file.
const DefaultSegmentSize = bitcask.DefaultSegmentSize

// DefaultCompactionRatio is the dead-byte fraction of a segment that
// triggers compaction.
const DefaultCompactionRatio = bitcask.DefaultCompactionRatio

// Open opens (creating if necessary) a DB rooted at opts.Dir. See
// DefaultOptions for the recommended configuration.
func Open(opts Options) (*DB, error) { return bitcask.Open(opts) }

// DefaultOptions returns the recommended configuration for dir: default
// segment size and compaction ratio, buffered (non-fsync-per-write)
// durability, and a snapshot written on clean Close.
func DefaultOptions(dir string) Options { return bitcask.DefaultOptions(dir) }
