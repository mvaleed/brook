package mmap

import (
	"fmt"
	"os"
	"syscall" // For production consider using: "golang.org/x/sys/unix"
)

type MmapStore struct {
	file *os.File
	data []byte
}

// NewMmapStore opens the file and maps it into memory.
// It handles the edge case where a new file is empty (0 bytes).
func NewMmapStore(path string) (*MmapStore, error) {
	// 1. Open the file in Read-Only mode.
	// We use standard os.Open which implies O_RDONLY.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// 2. Get the file size.
	fi, err := f.Stat()
	if err != nil {
		f.Close() // Don't leak the fd if stat fails
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	size := fi.Size()

	// 3. Handle Empty File (New Index).
	// syscall.Mmap returns an error (EINVAL) if size is 0.
	// We return a valid struct with a nil data slice.
	if size == 0 {
		return &MmapStore{
			file: f,
			data: nil,
		}, nil
	}

	// 4. Perform the Memory Map.
	// PROT_READ: We only intend to read.
	// MAP_SHARED: Changes by the Writer (in another process/handle) become visible here.
	data, err := syscall.Mmap(
		int(f.Fd()),
		0,
		int(size),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap: %w", err)
	}

	return &MmapStore{
		file: f,
		data: data,
	}, nil
}

// Sync checks if the file has grown and remaps it if necessary.
// Call this periodically or when a lookup fails to find an expected offset.
func (m *MmapStore) Sync() error {
	stat, err := m.file.Stat()
	if err != nil {
		return err
	}

	currentSize := stat.Size()

	if currentSize <= int64(len(m.data)) {
		return nil
	}

	// 3. Unmap the old view (if it exists)
	if len(m.data) > 0 {
		if err = syscall.Munmap(m.data); err != nil {
			return fmt.Errorf("munmap failed: %w", err)
		}
	}

	data, err := syscall.Mmap(
		int(m.file.Fd()),
		0,
		int(currentSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		m.data = nil
		return fmt.Errorf("remap failed, we lost our map. index is now broken: %w", err)
	}
	m.data = data

	return nil
}

// Close cleans up the memory map and closes the file handle.
func (m *MmapStore) Close() error {
	// 1. Unmap memory first (if mapped)
	if len(m.data) > 0 {
		if err := syscall.Munmap(m.data); err != nil {
			// Try to close file anyway before returning
			m.file.Close()
			return fmt.Errorf("munmap failed: %w", err)
		}
		m.data = nil
	}

	// 2. Close the file descriptor
	return m.file.Close()
}

// ReadAt returns a slice of the mmap data.
// It ensures you don't crash by reading out of bounds.
func (m *MmapStore) ReadAt(offset int, length int) ([]byte, error) {
	if m.data == nil {
		return nil, fmt.Errorf("storage is empty/closed")
	}

	if offset+length > len(m.data) {
		return nil, fmt.Errorf("out of bounds: len=%d, req_off=%d, req_len=%d", len(m.data), offset, length)
	}

	// Return the view (Zero Copy)
	return m.data[offset : offset+length], nil
}

func (m *MmapStore) Size() int64 {
	return int64(len(m.data))
}
