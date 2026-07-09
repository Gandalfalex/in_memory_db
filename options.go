package kv

// DefaultSegmentSize is the rotation threshold for a new active segment
// file. 64MB keeps a single segment's mmap footprint, and a compaction
// pass over it, small relative to a constrained (e.g. 1GB RAM, 1 CPU)
// host, while keeping the number of segment files/mmap regions reasonable
// at multi-GB total data sizes.
const DefaultSegmentSize int64 = 64 << 20

// DefaultCompactionRatio is the dead-byte fraction of a segment that
// triggers compaction.
const DefaultCompactionRatio = 0.5

// Options configures a DB. Zero-value numeric fields are replaced with
// defaults by Open; the two bool fields have no implicit default (a plain
// Options{Dir: "..."} literal leaves them false) — use DefaultOptions to
// get the recommended durability/maintenance posture instead of assuming
// a bare struct literal is fully configured.
type Options struct {
	// Dir is the directory holding segment files and the index snapshot.
	// Required.
	Dir string
	// SegmentSize is the byte size a segment is preallocated to before
	// rotating to a new one. Defaults to DefaultSegmentSize.
	SegmentSize int64
	// SyncOnWrite fsyncs the active segment after every Put/Delete.
	// Durable but slower; off by default (periodic/close-time sync only).
	SyncOnWrite bool
	// SnapshotOnClose writes an index checkpoint on a clean Close so the
	// next Open can skip re-scanning already-checkpointed segments.
	SnapshotOnClose bool
	// CompactionRatio is the dead-byte fraction (0-1) of an immutable
	// segment that marks it eligible for compaction. Defaults to
	// DefaultCompactionRatio.
	CompactionRatio float64
}

// DefaultOptions returns the recommended configuration for dir: default
// segment size and compaction ratio, buffered (non-fsync-per-write)
// durability, and a snapshot written on clean Close.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:             dir,
		SegmentSize:     DefaultSegmentSize,
		SyncOnWrite:     false,
		SnapshotOnClose: true,
		CompactionRatio: DefaultCompactionRatio,
	}
}

func (o Options) withDefaults() Options {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.CompactionRatio <= 0 {
		o.CompactionRatio = DefaultCompactionRatio
	}
	return o
}
