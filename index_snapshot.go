package kv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Index snapshot on-disk format (all integers big-endian):
//
//	magic "KVS1"(4) | version(4) | watermarkSegID(4) | watermarkSize(8) | keyCount(8)
//	keyCount * [ keyLen(2) | key | segID(4) | valOffset(4) | valLen(4) ]
//	crc32(4)  -- IEEE checksum over every byte before it
//
// This intentionally does not serialize the in-memory index's raw slot
// table/arena layout directly (which the design doc sketches as a
// possible optimization) — a flat list of (key, location) entries is
// decoupled from index.go's internal representation, simpler to get right,
// and just as sufficient for the recovery fast path: entries are re-applied
// through the normal idx.put path, exercising the same code as live writes.
//
// watermarkSegID/watermarkSize record how much of the *active* segment at
// snapshot time is captured: everything in every other (immutable) segment
// is assumed unchanged since snapshot time. This assumption is only safe
// because compaction.go deletes this file whenever it mutates segment
// layout — see recover() in recovery.go for the validity check this
// depends on.
const (
	snapshotFileName = "index.snapshot"
	snapshotMagic    = "KVS1"
	snapshotVersion  = 1
)

func snapshotPath(dir string) string {
	return filepath.Join(dir, snapshotFileName)
}

// saveSnapshot writes the current index to disk as a checkpoint. Caller
// must hold db.mu (at least a read lock; Close holds a write lock).
func (db *DB) saveSnapshot() error {
	tmpPath := snapshotPath(db.opts.Dir) + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriter(f)
	crc := crc32.NewIEEE()
	w := io.MultiWriter(bw, crc)

	if _, err := io.WriteString(w, snapshotMagic); err != nil {
		return err
	}
	if err := writeUint32(w, snapshotVersion); err != nil {
		return err
	}
	if err := writeUint32(w, db.active.id); err != nil {
		return err
	}
	if err := writeUint64(w, uint64(db.active.size)); err != nil {
		return err
	}
	if err := writeUint64(w, uint64(db.idx.len())); err != nil {
		return err
	}

	// One reused scratch buffer per entry avoids millions of tiny
	// allocations when checkpointing a large index.
	scratch := make([]byte, 0, 512)
	var walkErr error
	db.idx.forEach(func(key []byte, loc location) bool {
		entryLen := 2 + len(key) + 4 + 4 + 4
		if cap(scratch) < entryLen {
			scratch = make([]byte, entryLen)
		} else {
			scratch = scratch[:entryLen]
		}
		binary.BigEndian.PutUint16(scratch[0:2], uint16(len(key)))
		copy(scratch[2:2+len(key)], key)
		o := 2 + len(key)
		binary.BigEndian.PutUint32(scratch[o:o+4], loc.segID)
		binary.BigEndian.PutUint32(scratch[o+4:o+8], loc.valOffset)
		binary.BigEndian.PutUint32(scratch[o+8:o+12], loc.valLen)
		if _, err := w.Write(scratch); err != nil {
			walkErr = err
			return false
		}
		return true
	})
	if walkErr != nil {
		return walkErr
	}

	if err := binary.Write(bw, binary.BigEndian, crc.Sum32()); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	ok = true
	return os.Rename(tmpPath, snapshotPath(db.opts.Dir))
}

// verifySnapshotChecksum does a first streaming pass over the snapshot
// file to confirm its trailing CRC32 matches before any of its contents
// are trusted enough to mutate the live index.
func verifySnapshotChecksum(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 4 {
		return fmt.Errorf("kv: snapshot: truncated")
	}
	crc := crc32.NewIEEE()
	if _, err := io.CopyN(crc, f, info.Size()-4); err != nil {
		return fmt.Errorf("kv: snapshot: %w", err)
	}
	var stored [4]byte
	if _, err := io.ReadFull(f, stored[:]); err != nil {
		return fmt.Errorf("kv: snapshot: %w", err)
	}
	if binary.BigEndian.Uint32(stored[:]) != crc.Sum32() {
		return fmt.Errorf("kv: snapshot: checksum mismatch")
	}
	return nil
}

// loadSnapshotInto verifies and parses the snapshot at dir, applying every
// entry directly into idx as it streams (via idx.put, which copies key
// bytes into its own arena immediately, so a single reused scratch buffer
// suffices here too). Returns the watermark segment/size recorded at
// snapshot time.
func loadSnapshotInto(dir string, idx *index) (watermarkSegID uint32, watermarkSize int64, err error) {
	path := snapshotPath(dir)
	if err := verifySnapshotChecksum(path); err != nil {
		return 0, 0, err
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return 0, 0, fmt.Errorf("kv: snapshot: %w", err)
	}
	if string(magic) != snapshotMagic {
		return 0, 0, fmt.Errorf("kv: snapshot: bad magic")
	}
	version, err := readUint32(r)
	if err != nil {
		return 0, 0, err
	}
	if version != snapshotVersion {
		return 0, 0, fmt.Errorf("kv: snapshot: unsupported version %d", version)
	}
	watermarkSegID, err = readUint32(r)
	if err != nil {
		return 0, 0, err
	}
	sizeU64, err := readUint64(r)
	if err != nil {
		return 0, 0, err
	}
	watermarkSize = int64(sizeU64)
	count, err := readUint64(r)
	if err != nil {
		return 0, 0, err
	}

	keyBuf := make([]byte, MaxKeyLen)
	for range count {
		keyLen, err := readUint16(r)
		if err != nil {
			return 0, 0, err
		}
		key := keyBuf[:keyLen]
		if _, err := io.ReadFull(r, key); err != nil {
			return 0, 0, err
		}
		segID, err := readUint32(r)
		if err != nil {
			return 0, 0, err
		}
		valOffset, err := readUint32(r)
		if err != nil {
			return 0, 0, err
		}
		valLen, err := readUint32(r)
		if err != nil {
			return 0, 0, err
		}
		idx.put(key, location{segID: segID, valOffset: valOffset, valLen: valLen})
	}
	return watermarkSegID, watermarkSize, nil
}
