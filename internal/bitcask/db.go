package bitcask

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

// DB is an embedded, single-process key-value store: an append-only,
// memory-mapped segment log on disk plus a compact in-memory index (see
// index.go). See README for the full design rationale.
type DB struct {
	opts Options

	mu         sync.RWMutex
	idx        *index
	segments   map[uint32]*segment
	active     *segment
	compactOut *segment // current write target for compaction relocations, see compaction.go
	nextSegID  uint32
	deadBytes  map[uint32]int64 // segID -> bytes made garbage by later overwrites/deletes, read by compaction.go
	compactErr error            // outcome of the most recent compaction pass, surfaced via Stats
	closed     bool

	compactorStop chan struct{}
	compactorDone chan struct{}
}

// Open opens (creating if necessary) a DB rooted at opts.Dir. See
// DefaultOptions for the recommended configuration.
func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, fmt.Errorf("kv: Options.Dir is required")
	}
	if opts.CompactionRatio > 1 {
		return nil, fmt.Errorf("kv: Options.CompactionRatio %v out of range (0, 1]", opts.CompactionRatio)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("kv: create dir: %w", err)
	}

	db := &DB{
		opts:      opts,
		idx:       newIndex(),
		segments:  make(map[uint32]*segment),
		deadBytes: make(map[uint32]int64),
	}
	if err := db.recover(); err != nil {
		return nil, err
	}
	db.startCompactor()
	return db, nil
}

// Close stops background compaction, optionally snapshots the index, and
// syncs/closes every segment. Safe to call once; a second call returns
// ErrClosed.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return kvtypes.ErrClosed
	}
	db.closed = true
	db.mu.Unlock()

	db.stopCompactor()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.opts.SnapshotOnClose {
		if err := db.saveSnapshot(); err != nil {
			return fmt.Errorf("kv: snapshot on close: %w", err)
		}
	}
	for _, seg := range db.segments {
		if err := seg.sync(); err != nil {
			return fmt.Errorf("kv: sync segment %d: %w", seg.id, err)
		}
		if err := seg.close(); err != nil {
			return fmt.Errorf("kv: close segment %d: %w", seg.id, err)
		}
	}
	return nil
}

// Sync fsyncs the active segment, making every previously written record
// durable. Only useful when SyncOnWrite is off (with it on, every
// Put/Delete already syncs).
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kvtypes.ErrClosed
	}
	return db.active.sync()
}

// Stats is a point-in-time snapshot of the DB's size counters.
type Stats struct {
	// Keys is the number of live keys.
	Keys int
	// Segments is the number of open segment files, including the active
	// write segment and any compaction output segment.
	Segments int
	// DeadBytes is the total bytes superseded by later overwrites or
	// deletes, reclaimable by compaction.
	DeadBytes int64
	// LastCompactionErr is the error from the most recent compaction
	// pass (background or explicit Compact), nil if it succeeded. The
	// background compactor retries every tick, so a persistent non-nil
	// value here means dead bytes are accumulating unreclaimed.
	LastCompactionErr error
}

// Stats returns current size counters. Cheap: one read lock, no I/O.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := Stats{Keys: db.idx.len(), Segments: len(db.segments), LastCompactionErr: db.compactErr}
	for _, n := range db.deadBytes {
		s.DeadBytes += n
	}
	return s
}

// ensureCapacity rotates to a fresh active segment if a record of
// recordLen bytes wouldn't fit in the current one. Caller must hold db.mu.
func (db *DB) ensureCapacity(recordLen int64) error {
	if recordLen > db.opts.SegmentSize {
		return fmt.Errorf("kv: record of %d bytes exceeds segment size %d", recordLen, db.opts.SegmentSize)
	}
	if db.active.remaining() >= recordLen {
		return nil
	}
	if err := db.active.finalize(); err != nil {
		return err
	}
	seg, err := createSegment(db.opts.Dir, db.nextSegID, db.opts.SegmentSize)
	if err != nil {
		return err
	}
	db.segments[seg.id] = seg
	db.active = seg
	db.nextSegID++
	return nil
}

// addDeadBytes records that n bytes within segID are no longer live
// (superseded by a later write or deleted). Read by the background
// compactor (compaction.go) to decide which immutable segments to reclaim.
func (db *DB) addDeadBytes(segID uint32, n int64) {
	db.deadBytes[segID] += n
}

func listSegmentIDs(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ".seg"), 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(id))
	}
	slices.Sort(ids)
	return ids, nil
}
