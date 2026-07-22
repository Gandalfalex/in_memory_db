package bitcask

import (
	"os"
	"time"

	"github.com/Gandalfalex/in_memory_db/internal/kvtypes"
)

// compactionCheckInterval is how often the background compactor looks for
// an eligible segment. Coarse-grained on purpose: on a single CPU there's
// no benefit to checking often, and compaction itself is opportunistic
// (dead-byte counters are updated for free on every write; this ticker
// just decides when to act on them).
const compactionCheckInterval = 30 * time.Second

func (db *DB) startCompactor() {
	db.compactorStop = make(chan struct{})
	db.compactorDone = make(chan struct{})
	go db.compactionLoop()
}

func (db *DB) stopCompactor() {
	close(db.compactorStop)
	<-db.compactorDone
}

func (db *DB) compactionLoop() {
	defer close(db.compactorDone)
	ticker := time.NewTicker(compactionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.compactorStop:
			return
		case <-ticker.C:
			db.recordCompactionErr(db.compactNow())
		}
	}
}

// Compact synchronously runs compaction passes until no segment remains
// above CompactionRatio, without waiting for the background ticker.
func (db *DB) Compact() error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return kvtypes.ErrClosed
	}
	err := db.compactNow()
	db.recordCompactionErr(err)
	return err
}

// recordCompactionErr publishes the outcome of a compaction pass for
// Stats.LastCompactionErr; a nil err clears any earlier failure.
func (db *DB) recordCompactionErr(err error) {
	db.mu.Lock()
	db.compactErr = err
	db.mu.Unlock()
}

// compactNow runs compaction passes until no segment remains above the
// configured dead-byte ratio. Used by the background ticker, by Compact,
// and directly by tests that need deterministic compaction without
// waiting on the timer.
func (db *DB) compactNow() error {
	for {
		id, ok := db.pickCompactionCandidate()
		if !ok {
			return nil
		}
		if err := db.compactSegment(id); err != nil {
			return err
		}
	}
}

// pickCompactionCandidate returns an immutable segment whose dead-byte
// ratio meets the configured threshold, if any. The active write segment
// and the current compaction output segment are never candidates.
func (db *DB) pickCompactionCandidate() (uint32, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for id, seg := range db.segments {
		if db.active != nil && id == db.active.id {
			continue
		}
		if db.compactOut != nil && id == db.compactOut.id {
			continue
		}
		if seg.capacity == 0 {
			continue
		}
		ratio := float64(db.deadBytes[id]) / float64(seg.capacity)
		if ratio >= db.opts.CompactionRatio {
			return id, true
		}
	}
	return 0, false
}

// compactSegment streams every record in src, and for each one still live
// (the index still points at exactly this record) relocates it into the
// shared compaction-output segment and flips the index pointer — all in
// one locked critical section per record, so there is never a window where
// the index points at a location mid-move. Records that are no longer live
// (superseded or deleted since the scan started) are simply skipped: their
// bytes are already accounted for as dead and are dropped along with src.
//
// Once every live record has been relocated, src is deleted. Any existing
// index snapshot is also deleted here: a snapshot's fast-recovery path
// (recovery.go) assumes every immutable segment below its watermark is
// unchanged since it was written, and compaction just violated that for
// src (and possibly the compaction-output segment), so the snapshot can no
// longer be trusted.
func (db *DB) compactSegment(srcID uint32) error {
	db.mu.Lock()
	src, ok := db.segments[srcID]
	if !ok || (db.active != nil && src.id == db.active.id) {
		db.mu.Unlock()
		return nil // raced away since it was picked; nothing to do
	}
	db.mu.Unlock()

	_, err := src.forEach(0, func(off int64, h recordHeader, key, value []byte) error {
		if h.tombstone() {
			return nil // tombstones never carry forward
		}

		db.mu.Lock()
		defer db.mu.Unlock()

		cur, found := db.idx.get(key)
		wantOffset := uint32(off + headerSize + int64(h.keyLen))
		if !found || cur.segID != src.id || cur.valOffset != wantOffset {
			return nil // superseded since the scan started; skip
		}

		if err := db.ensureCompactionCapacity(recordSize(h.keyLen, h.valLen)); err != nil {
			return err
		}
		record := encodeRecord(key, value, false, h.timestamp)
		newOff, err := db.compactOut.append(record)
		if err != nil {
			return err
		}
		newValOffset := newOff + headerSize + int64(len(key))
		db.idx.put(key, location{segID: db.compactOut.id, valOffset: uint32(newValOffset), valLen: h.valLen})
		return nil
	})
	if err != nil {
		return err
	}

	db.mu.Lock()
	delete(db.deadBytes, src.id)
	delete(db.segments, src.id)
	db.mu.Unlock()

	if err := src.remove(); err != nil {
		return err
	}
	_ = os.Remove(snapshotPath(db.opts.Dir))
	return nil
}

// ensureCompactionCapacity lazily creates the compaction-output segment on
// first use and rotates it once full, mirroring ensureCapacity's role for
// the active write segment. Caller must hold db.mu.
func (db *DB) ensureCompactionCapacity(recordLen int64) error {
	if db.compactOut != nil && db.compactOut.remaining() >= recordLen {
		return nil
	}
	if db.compactOut != nil {
		if err := db.compactOut.finalize(); err != nil {
			return err
		}
	}
	seg, err := createSegment(db.opts.Dir, db.nextSegID, db.opts.SegmentSize)
	if err != nil {
		return err
	}
	db.segments[seg.id] = seg
	db.compactOut = seg
	db.nextSegID++
	return nil
}
