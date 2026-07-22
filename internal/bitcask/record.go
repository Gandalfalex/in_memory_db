package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

// On-disk record layout (all integers big-endian):
//
//	CRC32(4) | Timestamp(8) | KeyLen(4) | ValLen(4) | Flags(1) | Key | Value
//
// CRC32 is computed over everything after itself (Timestamp..Value) and
// lets recovery distinguish a genuine record from a torn write or
// unwritten (zeroed) preallocated space: CRC-32 IEEE starts from an
// all-ones register and applies a final XOR, so an all-zero span never
// checksums to a stored value of 0.
const headerSize = 21

const flagTombstone byte = 1 << 0

type recordHeader struct {
	crc       uint32
	timestamp int64
	keyLen    uint32
	valLen    uint32
	flags     byte
}

func (h recordHeader) tombstone() bool { return h.flags&flagTombstone != 0 }

func recordSize(keyLen, valLen uint32) int64 {
	return int64(headerSize) + int64(keyLen) + int64(valLen)
}

// encodeRecord serializes a full record (header+key+value) as a new slice.
func encodeRecord(key, value []byte, tombstone bool, timestamp int64) []byte {
	buf := make([]byte, recordSize(uint32(len(key)), uint32(len(value))))

	var flags byte
	if tombstone {
		flags = flagTombstone
	}
	binary.BigEndian.PutUint64(buf[4:12], uint64(timestamp))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(value)))
	buf[20] = flags
	copy(buf[headerSize:headerSize+len(key)], key)
	copy(buf[headerSize+len(key):], value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)
	return buf
}

// decodeHeader parses the fixed header. buf must be at least headerSize
// bytes; callers (segment.forEach) guarantee this before calling.
func decodeHeader(buf []byte) recordHeader {
	return recordHeader{
		crc:       binary.BigEndian.Uint32(buf[0:4]),
		timestamp: int64(binary.BigEndian.Uint64(buf[4:12])),
		keyLen:    binary.BigEndian.Uint32(buf[12:16]),
		valLen:    binary.BigEndian.Uint32(buf[16:20]),
		flags:     buf[20],
	}
}

// verifyChecksum reports whether a full record's stored CRC32 matches the
// checksum recomputed over its Timestamp..Value bytes.
func verifyChecksum(record []byte) bool {
	if len(record) < headerSize {
		return false
	}
	stored := binary.BigEndian.Uint32(record[0:4])
	return crc32.ChecksumIEEE(record[4:]) == stored
}
