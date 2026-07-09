//go:build unix

// Package mmapfile provides a fixed-capacity memory-mapped file: the backing
// file is pre-allocated to a fixed size (typically a segment's rotation
// threshold) and mapped once, so appends are plain slice writes and no
// remapping is needed for the lifetime of the segment.
package mmapfile

import (
	"fmt"
	"os"
	"syscall"
)

type File struct {
	f    *os.File
	data []byte
}

// Create makes a new file at path, preallocates it to size bytes, and maps
// the whole range MAP_SHARED so writes into Bytes() are visible to any
// other reader of the same file via the OS page cache.
func Create(path string, size int64) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	return mapFile(f, size)
}

// Open maps an existing file. size is the capacity to map; the file is
// truncated (extended) up to size if it is currently smaller.
func Open(path string, size int64) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < size {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, err
		}
	} else if info.Size() > size {
		size = info.Size()
	}
	return mapFile(f, size)
}

var pageSize = int64(os.Getpagesize())

// roundUpToPage rounds n up to the next page boundary. Used to pad the
// length requested from mmap(2): the file may be an arbitrary (unaligned)
// number of bytes, but mmap works in whole pages. Mapping only exactly
// `size` bytes still gets rounded up to a page internally by the kernel,
// but Go's copy()/memmove can, for a handful of bytes near the end of a
// small buffer, read slightly past the logical slice length using a wider
// (word-sized) load. As long as that over-read stays inside the *same*
// trailing page the data lives in, it's harmless (the kernel zero-fills
// the tail of the last mapped page beyond EOF); explicitly requesting the
// mapping out to that page boundary — instead of relying on it being an
// accidental side effect — is what makes that guarantee dependable rather
// than incidental. See README's "mmap vs pread" section for the source of
// this pattern (a documented SIGBUS bug in VictoriaMetrics).
func roundUpToPage(n int64) int64 {
	if rem := n % pageSize; rem != 0 {
		n += pageSize - rem
	}
	return n
}

func mapFile(f *os.File, size int64) (*File, error) {
	if size == 0 {
		return &File{f: f, data: nil}, nil
	}
	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(roundUpToPage(size)), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmapfile: mmap %s: %w", f.Name(), err)
	}
	// Slice down to the logical size: callers only ever see exactly what
	// they asked for, while cap() retains the full page-rounded region for
	// Close/Truncate to unmap correctly.
	return &File{f: f, data: mapped[:size]}, nil
}

// Bytes returns the full mapped region. Writes into the returned slice are
// written through to the file (MAP_SHARED); callers must track how much of
// it is actually in use themselves (mmapfile has no concept of "used
// length", only mapped capacity).
func (m *File) Bytes() []byte {
	return m.data
}

// ReadAt does a positioned read of the underlying file via pread(2),
// independent of the memory mapping. Unlike touching Bytes(), this is a
// syscall the Go runtime scheduler recognizes as blocking: if the target
// range isn't in the page cache, the OS thread can be detached from its P
// so other goroutines keep running, instead of the P being stuck behind an
// invisible major page fault. Callers that don't know whether a range is
// resident should prefer this over slicing Bytes() directly — see
// pagesResident in mincore_unix.go.
func (m *File) ReadAt(p []byte, off int64) (int, error) {
	return m.f.ReadAt(p, off)
}

// Sync flushes the file to disk. For a MAP_SHARED mapping, writes into
// Bytes() dirty the same page-cache pages backing the file, so fsync(2) on
// the fd flushes them without a separate msync step.
func (m *File) Sync() error {
	return m.f.Sync()
}

// Truncate resizes the backing file and remaps it to the new size. Used
// when shrinking a rotated-out segment down to its actual used length, or
// growing capacity.
func (m *File) Truncate(size int64) error {
	if len(m.data) > 0 {
		if err := syscall.Munmap(m.data[:cap(m.data)]); err != nil {
			return err
		}
		m.data = nil
	}
	if err := m.f.Truncate(size); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	mapped, err := syscall.Mmap(int(m.f.Fd()), 0, int(roundUpToPage(size)), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmapfile: remap %s: %w", m.f.Name(), err)
	}
	m.data = mapped[:size]
	return nil
}

// Close unmaps and closes the underlying file.
func (m *File) Close() error {
	var errs []error
	if len(m.data) > 0 {
		if err := syscall.Munmap(m.data[:cap(m.data)]); err != nil {
			errs = append(errs, err)
		}
		m.data = nil
	}
	if err := m.f.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("mmapfile: close: %v", errs)
	}
	return nil
}
