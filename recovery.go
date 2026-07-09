package kv

import (
	"fmt"
	"os"
)

// recover brings db.idx and db.segments up to date with what's on disk. It
// tries the index snapshot fast path first (load the checkpoint, then
// replay only the bytes appended to the active segment since); if no valid
// snapshot is available it falls back to a full scan of every segment from
// the start. Called once from Open, before the background compactor
// starts.
func (db *DB) recover() error {
	ids, err := listSegmentIDs(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("kv: list segments: %w", err)
	}

	if len(ids) == 0 {
		seg, err := createSegment(db.opts.Dir, 0, db.opts.SegmentSize)
		if err != nil {
			return err
		}
		db.segments[0] = seg
		db.active = seg
		db.nextSegID = 1
		return nil
	}

	present := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		present[id] = true
	}

	watermarkSegID, watermarkSize, useSnapshot := db.tryLoadSnapshot(present)

	for i, id := range ids {
		seg, err := openSegment(db.opts.Dir, id)
		if err != nil {
			return err
		}
		db.segments[id] = seg

		switch {
		case useSnapshot && id < watermarkSegID:
			// Fully covered by the snapshot (see index_snapshot.go for why
			// this is safe: compaction invalidates the snapshot whenever it
			// changes segment layout, so an accepted snapshot's view of
			// every segment below its watermark is still current). Such a
			// segment was finalized (immutable) by the time the snapshot
			// was written, so its on-disk size already equals its used size.
			seg.size = seg.capacity
		case useSnapshot && id == watermarkSegID:
			end, err := db.replaySegment(seg, watermarkSize)
			if err != nil {
				return err
			}
			seg.size = end
		default:
			end, err := db.replaySegment(seg, 0)
			if err != nil {
				return err
			}
			seg.size = end
		}

		if i == len(ids)-1 {
			db.active = seg
		} else if err := seg.finalize(); err != nil {
			return err
		}
	}
	db.nextSegID = ids[len(ids)-1] + 1
	return nil
}

// tryLoadSnapshot attempts the fast-path recovery: load and verify the
// snapshot file, sanity-check its watermark segment still exists, and
// apply its entries into db.idx. On any failure it discards whatever was
// partially applied and returns useSnapshot=false so recover() falls back
// to a full scan of every segment.
func (db *DB) tryLoadSnapshot(present map[uint32]bool) (watermarkSegID uint32, watermarkSize int64, useSnapshot bool) {
	if _, err := os.Stat(snapshotPath(db.opts.Dir)); err != nil {
		return 0, 0, false
	}
	watermarkSegID, watermarkSize, err := loadSnapshotInto(db.opts.Dir, db.idx)
	if err != nil || !present[watermarkSegID] {
		db.idx = newIndex() // discard any partial/unverified state
		return 0, 0, false
	}
	return watermarkSegID, watermarkSize, true
}

// replaySegment scans seg starting at fromOffset, applying each
// well-formed record to db.idx (put for a value, delete for a tombstone),
// and returns how many leading bytes of the segment are valid data — that
// becomes the segment's used size.
func (db *DB) replaySegment(seg *segment, fromOffset int64) (int64, error) {
	return seg.forEach(fromOffset, func(off int64, h recordHeader, key, value []byte) error {
		if h.tombstone() {
			db.idx.delete(key)
		} else {
			valOffset := off + headerSize + int64(h.keyLen)
			db.idx.put(key, location{segID: seg.id, valOffset: uint32(valOffset), valLen: h.valLen})
		}
		return nil
	})
}
