package kv

import (
	"bytes"
	"hash/maphash"
)

// location is where a value currently lives on disk: which segment, the
// byte offset of the value itself (not the record header) within that
// segment, and its length — enough for a caller to do a direct read with
// no re-parsing of the record header.
type location struct {
	segID     uint32
	valOffset uint32
	valLen    uint32
}

// index is an in-memory, open-addressing hash table mapping key -> location.
//
// It deliberately does not use a plain Go map[string]indexEntry. At a few
// million keys the byte savings of a custom structure over a map are
// modest (~5-9%, since key bytes dominate either way) — the real reason is
// GC pressure: map[string]T holds one string header (a pointer) per entry,
// which the garbage collector must visit on every mark phase. On a
// single-CPU host with no spare core to absorb that scan cost, millions of
// per-entry pointers is a real, avoidable tax. Here, keys live in a byte
// arena and slots are a flat, pointer-free array, so the whole index is
// effectively invisible to the GC scanner (a handful of large backing
// arrays instead of millions of small objects).
type index struct {
	seed  maphash.Seed
	slots []indexSlot
	mask  uint64 // len(slots)-1; len(slots) is always a power of two
	count int    // live entries
	tomb  int    // tombstoned (deleted) slots, reclaimed on next rehash
	arena *arena
}

type slotState uint8

const (
	slotEmpty slotState = iota
	slotLive
	slotTombstone
)

// indexSlot is the fixed-size, pointer-free entry stored per hash bucket.
type indexSlot struct {
	hash      uint64
	state     slotState
	chunkIdx  uint16 // which arena chunk the key lives in
	keyOffset uint32 // byte offset of the key within that chunk
	keyLen    uint16 // key length in bytes (64KB ceiling, documented in README)
	loc       location
}

const (
	initialSlotCapacity = 1024
	maxLoadFactor       = 0.65
)

func newIndex() *index {
	return &index{
		seed:  maphash.MakeSeed(),
		slots: make([]indexSlot, initialSlotCapacity),
		mask:  initialSlotCapacity - 1,
		arena: newArena(arenaChunkSize),
	}
}

func (idx *index) hash(key []byte) uint64 {
	return maphash.Bytes(idx.seed, key)
}

func (idx *index) keyAt(s *indexSlot) []byte {
	return idx.arena.get(s.chunkIdx, s.keyOffset, s.keyLen)
}

// get returns the current on-disk location of key, if live.
func (idx *index) get(key []byte) (location, bool) {
	h := idx.hash(key)
	i := h & idx.mask
	for probes := uint64(0); probes <= idx.mask; probes++ {
		s := &idx.slots[i]
		switch s.state {
		case slotEmpty:
			return location{}, false
		case slotLive:
			if s.hash == h && bytes.Equal(idx.keyAt(s), key) {
				return s.loc, true
			}
		}
		i = (i + 1) & idx.mask
	}
	return location{}, false
}

// put inserts or overwrites key's location. If key was already live, its
// previous location is returned (hadPrev=true) so the caller can update
// dead-byte accounting on the old segment for compaction.
func (idx *index) put(key []byte, loc location) (prev location, hadPrev bool) {
	idx.maybeRehash()

	h := idx.hash(key)
	i := h & idx.mask
	reuse := int64(-1)
	for probes := uint64(0); probes <= idx.mask; probes++ {
		s := &idx.slots[i]
		switch s.state {
		case slotEmpty:
			target := i
			if reuse >= 0 {
				target = uint64(reuse)
				idx.tomb--
			}
			idx.writeSlot(target, key, h, loc)
			idx.count++
			return location{}, false
		case slotTombstone:
			if reuse < 0 {
				reuse = int64(i)
			}
		case slotLive:
			if s.hash == h && bytes.Equal(idx.keyAt(s), key) {
				prev = s.loc
				s.loc = loc
				return prev, true
			}
		}
		i = (i + 1) & idx.mask
	}
	// Table full of tombstones/live entries with no empty slot found within
	// one full probe cycle: force a rehash (which drops tombstones and/or
	// grows) and retry once — cannot loop forever since rehash always
	// creates room.
	idx.rehash(idx.growTargetCapacity())
	return idx.put(key, loc)
}

// delete marks key's slot as a tombstone. If key was live, its location is
// returned (hadPrev=true) for dead-byte accounting.
func (idx *index) delete(key []byte) (prev location, hadPrev bool) {
	h := idx.hash(key)
	i := h & idx.mask
	for probes := uint64(0); probes <= idx.mask; probes++ {
		s := &idx.slots[i]
		switch s.state {
		case slotEmpty:
			return location{}, false
		case slotLive:
			if s.hash == h && bytes.Equal(idx.keyAt(s), key) {
				prev = s.loc
				s.state = slotTombstone
				idx.count--
				idx.tomb++
				return prev, true
			}
		}
		i = (i + 1) & idx.mask
	}
	return location{}, false
}

func (idx *index) writeSlot(i uint64, key []byte, h uint64, loc location) {
	chunkIdx, keyOffset := idx.arena.put(key)
	idx.slots[i] = indexSlot{
		hash:      h,
		state:     slotLive,
		chunkIdx:  chunkIdx,
		keyOffset: keyOffset,
		keyLen:    uint16(len(key)),
		loc:       loc,
	}
}

// maybeRehash grows or compacts the table when live+tombstone occupancy
// crosses the load factor threshold.
func (idx *index) maybeRehash() {
	occupied := idx.count + idx.tomb
	if float64(occupied) < maxLoadFactor*float64(len(idx.slots)) {
		return
	}
	idx.rehash(idx.growTargetCapacity())
}

// growTargetCapacity decides whether the next rehash should grow the table
// or just compact it in place. If live entries alone are already more than
// half of capacity, growing is genuinely needed; if occupancy is mostly
// tombstones (delete-heavy workload), rehashing at the same size reclaims
// them without spending extra memory on a larger table.
func (idx *index) growTargetCapacity() int {
	capacity := len(idx.slots)
	if idx.count*2 >= capacity {
		return capacity * 2
	}
	return capacity
}

func (idx *index) rehash(newCap int) {
	old := idx.slots
	idx.slots = make([]indexSlot, newCap)
	idx.mask = uint64(newCap - 1)
	idx.tomb = 0
	// count is unchanged (we're only relocating live entries), so reset and
	// let the reinsert loop rebuild it, catching any accounting drift.
	idx.count = 0
	for i := range old {
		if old[i].state != slotLive {
			continue
		}
		idx.reinsert(&old[i])
	}
}

// reinsert places an already-arena-backed slot into the current table
// without touching the arena (key bytes are never copied on rehash).
func (idx *index) reinsert(s *indexSlot) {
	i := s.hash & idx.mask
	for {
		if idx.slots[i].state == slotEmpty {
			idx.slots[i] = *s
			idx.count++
			return
		}
		i = (i + 1) & idx.mask
	}
}

// forEach calls fn for every live entry. fn returning false stops iteration
// early. Used by prefix scans and index snapshotting.
func (idx *index) forEach(fn func(key []byte, loc location) bool) {
	for i := range idx.slots {
		s := &idx.slots[i]
		if s.state != slotLive {
			continue
		}
		if !fn(idx.keyAt(s), s.loc) {
			return
		}
	}
}

func (idx *index) len() int {
	return idx.count
}
