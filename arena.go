package kv

// arena stores index key bytes in large, chunked backing slices instead of
// one small []byte allocation per key. A key never spans two chunks, so
// growth only ever appends a new chunk (or, for a key larger than the
// chunk size, a one-off oversized chunk) — existing chunks and the slot
// offsets pointing into them are never invalidated or copied.
const arenaChunkSize = 16 << 20 // 16MB

type arena struct {
	chunkSize int
	chunks    [][]byte
	used      int // bytes used in chunks[len(chunks)-1]
}

func newArena(chunkSize int) *arena {
	return &arena{chunkSize: chunkSize}
}

// put copies key into the arena and returns its location.
func (a *arena) put(key []byte) (chunkIdx uint16, offset uint32) {
	if len(a.chunks) == 0 || a.used+len(key) > len(a.chunks[len(a.chunks)-1]) {
		size := a.chunkSize
		if len(key) > size {
			size = len(key) // oversized key gets a dedicated, exactly-sized chunk
		}
		a.chunks = append(a.chunks, make([]byte, size))
		a.used = 0
	}
	idx := len(a.chunks) - 1
	off := a.used
	copy(a.chunks[idx][off:], key)
	a.used += len(key)
	return uint16(idx), uint32(off)
}

// get returns the key bytes at the given location.
func (a *arena) get(chunkIdx uint16, offset uint32, length uint16) []byte {
	return a.chunks[chunkIdx][offset : offset+uint32(length)]
}
