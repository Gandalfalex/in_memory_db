//go:build !unix

package bitcask

// pagesResident has no mincore(2) equivalent wired up on this platform.
// Since mmapfile's non-unix fallback isn't a real memory mapping anyway
// (see internal/mmapfile/mmapfile_other.go) — it's an already fully
// resident heap buffer — there's no page-fault-stall risk to guard
// against here, so segment.go's pread fallback (also just an in-memory
// copy on this platform) is harmless to always take.
func pagesResident(data []byte, offset, length int) bool {
	return false
}
