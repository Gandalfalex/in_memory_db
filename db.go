package kv

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
		return ErrClosed
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
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
