package storage

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/mvaleed/brook/internal/storage/mmap"
)

/*
  ALGORITHM: Sparse Index Lookup (Floor Search)
  ------------------------------------------------------------------
  Example Data:
  Entry 0: Offset 0    -> Pos 0
  Entry 1: Offset 500  -> Pos 1024
  Entry 2: Offset 1000 -> Pos 2048

  Goal: Find Log Entry with Offset 800.

  Part 1: The Index Search (Binary Search via mmap)
  1. Sync(): Ensure mmap reader sees the latest writes.
  2. Binary Search: Find the smallest index 'i' where Entry[i].Offset > 800.
     -> In this example, it finds Entry 2 (Offset 1000).
  3. The "Floor" Step: We want the range *containing* 800, so we take (i - 1).
     -> We select Entry 1 (Offset 500, Pos 1024).
  4. Return: We return the Physical Position (1024) to the caller.

  Part 2: The Log Scan (Performed by the Caller on the Log File)
  5. Caller takes the position (1024).
  6. Caller Seeks to position 1024 in the actual *.log file.
  7. Caller linearly scans messages (decoding headers) starting from 500.
  8. Caller stops when it finds Offset 800 (Success) or Offset > 800 (Not Found).
*/

type Index struct {
	// RWMutex allows multiple readers OR one writer.
	mu sync.RWMutex

	file             *os.File
	writer           *bufio.Writer
	writerBufferSize int
	reader           *mmap.MmapStore
}

func NewIndex(path string) (*Index, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	// Truncate corrupt tail if necessary
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size()%entryWidth != 0 {
		newSize := fi.Size() - (fi.Size() % entryWidth)
		if err := f.Truncate(newSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to truncate corrupt index tail: %w", err)
		}
	}

	reader, err := mmap.NewMmapStore(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	writerBufferSize := entryWidth * 5
	return &Index{
		file:             f,
		writer:           bufio.NewWriterSize(f, writerBufferSize),
		reader:           reader,
		writerBufferSize: writerBufferSize,
	}, nil
}

// WriteEntry appends a new entry.
// LOCK STRATEGY: Exclusive Lock (Lock).
func (i *Index) WriteEntry(entry IndexEntry) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	var buf [entryWidth]byte
	entry.Marshal(buf[:])

	_, err := i.writer.Write(buf[:])
	return err
}

// readEntryInternal is a private helper without locks.
func (i *Index) readEntryInternal(idx int) (IndexEntry, error) {
	offset := idx * entryWidth

	chunk, err := i.reader.ReadAt(offset, entryWidth)
	if err != nil {
		return IndexEntry{}, fmt.Errorf("failed to read entry %d: %w", idx, err)
	}

	indexEntry := IndexEntry{}
	indexEntry.Unmarshal(chunk)
	return indexEntry, nil
}

// FindNearest finds the closest offset.
// LOCK STRATEGY: Mixed.
// 1. Lock() to Sync (Writer Lock).
// 2. Downgrade to RLock() to Search (Reader Lock).
func (i *Index) FindNearest(targetOffset uint32) (IndexEntry, error) {
	// Sync the Reader (Needs Write Lock because Sync modifies mmap slice)
	// We wrap this in a closure or block to ensure Unlock happens immediately
	if err := func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Flush writer to disk so reader can see it
		if err := i.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		// Remap memory if file grew
		return i.reader.Sync()
	}(); err != nil {
		return IndexEntry{}, fmt.Errorf("failed to sync: %w", err)
	}

	// The Search (Needs Read Lock)
	i.mu.RLock()
	defer i.mu.RUnlock()

	totalEntries := int(i.reader.Size()) / entryWidth

	var readErr error
	idx := sort.Search(totalEntries, func(k int) bool {
		entry, err := i.readEntryInternal(k)
		if err != nil {
			readErr = err
		}
		return entry.LogicalOff > targetOffset
	})

	if readErr != nil {
		return IndexEntry{}, fmt.Errorf("failed to read index entry: %w", readErr)
	}

	if idx == 0 {
		return IndexEntry{}, nil
	}

	return i.readEntryInternal(idx - 1)
}

func (i *Index) LastEntry() (IndexEntry, error) {
	i.mu.Lock()
	if err := i.writer.Flush(); err != nil {
		i.mu.Unlock()
		return IndexEntry{}, err
	}
	if err := i.reader.Sync(); err != nil {
		i.mu.Unlock()
		return IndexEntry{}, err
	}
	i.mu.Unlock()

	i.mu.RLock()
	defer i.mu.RUnlock()

	size := i.reader.Size()
	if size == 0 {
		return IndexEntry{}, nil
	}

	lastIndex := int(size/entryWidth) - 1
	return i.readEntryInternal(lastIndex)
}

// Close flushes and cleans up.
// LOCK STRATEGY: Exclusive Lock.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := i.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush index writer: %w", err)
	}

	if err := i.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := i.reader.Close(); err != nil {
		return fmt.Errorf("failed to close reader: %w", err)
	}

	return i.file.Close()
}

func (i *Index) Flush() error {
	return i.writer.Flush()
}
