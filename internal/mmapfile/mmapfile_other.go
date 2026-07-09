//go:build !unix

// Fallback for platforms without mmap support (e.g. windows): Bytes()
// materializes the file contents into a regular heap-allocated slice and
// Sync() writes it back. This loses the zero-copy / OS-page-cache benefits
// of the unix implementation and is not the primary target of this
// package's memory-bound design; it exists so the package still builds and
// functions correctly on non-unix platforms.
package mmapfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type File struct {
	f    *os.File
	data []byte
}

func Create(path string, size int64) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	return &File{f: f, data: make([]byte, size)}, nil
}

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
	if info.Size() > size {
		size = info.Size()
	} else if info.Size() < size {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, err
		}
	}
	data := make([]byte, size)
	if size > 0 {
		if _, err := f.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
			f.Close()
			return nil, err
		}
	}
	return &File{f: f, data: data}, nil
}

func (m *File) Bytes() []byte {
	return m.data
}

// ReadAt reads directly from the already-resident in-memory copy. There's
// no real mapping on this fallback path (see package doc), so there's no
// page-fault-stall concern this needs to defend against — it exists only
// so this build shares mmapfile_unix.go's API.
func (m *File) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, fmt.Errorf("mmapfile: ReadAt offset %d out of range", off)
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *File) Sync() error {
	if len(m.data) > 0 {
		if _, err := m.f.WriteAt(m.data, 0); err != nil {
			return err
		}
	}
	return m.f.Sync()
}

func (m *File) Truncate(size int64) error {
	if err := m.Sync(); err != nil {
		return err
	}
	if err := m.f.Truncate(size); err != nil {
		return err
	}
	newData := make([]byte, size)
	n := int64(len(m.data))
	if n > size {
		n = size
	}
	copy(newData, m.data[:n])
	m.data = newData
	return nil
}

func (m *File) Close() error {
	if err := m.Sync(); err != nil {
		m.f.Close()
		return err
	}
	return m.f.Close()
}
