//go:build unix

package kv

import (
	"os"
	"syscall"
	"unsafe"
)

var systemPageSize = int64(os.Getpagesize())

// pagesResident reports whether every page covering data[offset:offset+length]
// is currently resident in RAM, via mincore(2). data must be backed by an
// mmap'd region (segment.mf.Bytes()).
//
// This exists because of a subtle Go runtime interaction: a memory load
// that touches a non-resident mmap'd page causes a major page fault, and
// the OS thread blocks until the kernel loads the page from disk. Unlike a
// blocking syscall (read/pread), the Go scheduler has no visibility into
// this — it just looks like a goroutine executing a normal instruction —
// so it can't detach the stuck OS thread and hand its P to another
// goroutine the way it would for a blocked syscall. On a host with only
// one CPU (this package's primary target), a single goroutine hitting a
// cold mmap'd page can stall the entire process, not just itself: there is
// no second P for other goroutines to run on while the fault resolves.
//
// mincore() itself is cheap and non-blocking (it just consults the
// kernel's page tables, no disk I/O), and — critically — it's a syscall
// the Go runtime *does* recognize as blocking-capable, so calling it can
// never introduce the failure mode above. segment.go's readAt uses this to
// gate: read straight from the mmap when the range is confirmed resident,
// otherwise fall back to a real pread (File.ReadAt), which the runtime can
// schedule around normally.
//
// This function itself is uncached — it always makes the syscall. Calling
// it on every read would mean paying mincore's cost (comparable to a
// pread's) even for pages already known hot, largely defeating the point.
// segment.go's cachedPagesResident wraps this with a per-segment residency
// bitmap (mirroring the pattern in the source article this design is
// adapted from — VictoriaMetrics/VictoriaLogs, see README) so repeat reads
// of the same pages skip the syscall entirely once confirmed resident.
func pagesResident(data []byte, offset, length int) bool {
	if length <= 0 || offset < 0 || int64(offset+length) > int64(len(data)) {
		return false
	}
	start := int64(offset) - int64(offset)%systemPageSize
	span := int64(offset+length) - start
	numPages := (span + systemPageSize - 1) / systemPageSize

	vec := make([]byte, numPages)
	_, _, errno := syscall.Syscall(
		syscall.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[start])),
		uintptr(span),
		uintptr(unsafe.Pointer(&vec[0])),
	)
	if errno != 0 {
		// Can't confirm residency (e.g. ENOMEM/EINVAL from a platform
		// quirk); the safe answer is "assume not resident" so the caller
		// takes the pread fallback instead of risking an unguarded fault.
		return false
	}
	for _, b := range vec {
		if b&1 == 0 {
			return false
		}
	}
	return true
}
