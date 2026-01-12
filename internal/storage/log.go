// Package storage is idk
package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	asyncwriter "github.com/mvaleed/brook/internal/storage/async-writer"
)

var ErrRecordNotFoundFullScan = errors.New("Record with offset not found after full scan")

type Log struct {
	mu            sync.RWMutex
	readOnly      bool
	file          *os.File
	path          string
	nextMemoryPos int64
	nextOffset    int64
	baseOffset    int64 // Represents global offset
	createdAt     time.Time
	writeFunc     func([]byte) (int, error)
	flushFunc     func() error
	closeFunc     func() error

	index     *Index
	indexPath string
}

func NewLogReadOnly(path string, baseOffset int) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	indexPath := path + ".index"
	index, err := NewIndex(indexPath)
	if err != nil {
		f.Close()
		return nil, err
	}

	lastEntry, err := index.LastEntry()
	if err != nil {
		f.Close()
		index.Close()
		return nil, err
	}

	l := &Log{
		file:          f,
		nextMemoryPos: info.Size(),
		nextOffset:    0,
		writeFunc: func([]byte) (int, error) {
			return 0, nil
		},
		flushFunc: func() error {
			return nil
		},
		closeFunc: func() error {
			return nil
		},
		index:      index,
		indexPath:  indexPath,
		path:       path,
		createdAt:  TimeNowInUtc(),
		readOnly:   true,
		baseOffset: int64(baseOffset),
	}
	if info.Size() != 0 {
		l.nextOffset, err = l.reloadNextOffset(lastEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize read only log: %w", err)
		}
		l.createdAt = info.ModTime()
	}

	return l, nil
}

func newLog(path string, baseOffset int, writerBufferSize int, flushToOSOnEveryAppend bool, flushToDiskOnEveryAppend bool) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	indexPath := path + ".index"
	index, err := NewIndex(indexPath)
	if err != nil {
		f.Close()
		return nil, err
	}

	lastEntry, err := index.LastEntry()
	if err != nil {
		f.Close()
		index.Close()
		return nil, err
	}
	var writeFunc func([]byte) (int, error)
	var flushFunc func() error
	var closeFunc func() error

	if flushToDiskOnEveryAppend || flushToOSOnEveryAppend {
		// Synchronous modes - use bufio.Writer
		writer := bufio.NewWriterSize(f, writerBufferSize)

		writeFunc = func(data []byte) (int, error) {
			n, err := writer.Write(data)
			if err != nil {
				return n, err
			}
			if flushToOSOnEveryAppend {
				if err := writer.Flush(); err != nil {
					return 0, err
				}
			}
			if flushToDiskOnEveryAppend {
				if err := f.Sync(); err != nil {
					return 0, err
				}
			}
			return n, nil
		}

		flushFunc = func() error { return writer.Flush() }
		closeFunc = func() error { return writer.Flush() }
	} else {
		// Async mode - use AsyncWriter with periodic flushing
		asyncWriter := asyncwriter.NewAsyncWriterSize(f, writerBufferSize)

		writeFunc = func(data []byte) (int, error) {
			return asyncWriter.Write(data)
		}
		flushFunc = func() error { return asyncWriter.Flush() }
		closeFunc = func() error { return asyncWriter.Close() }
	}

	l := &Log{
		file:          f,
		nextMemoryPos: info.Size(),
		nextOffset:    0,
		writeFunc:     writeFunc,
		flushFunc:     flushFunc,
		closeFunc:     closeFunc,
		index:         index,
		indexPath:     indexPath,
		path:          path,
		createdAt:     TimeNowInUtc(),
		readOnly:      false,
		baseOffset:    int64(baseOffset),
	}

	if info.Size() != 0 {
		l.nextOffset, err = l.reloadNextOffset(lastEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize log: %w", err)
		}
		l.createdAt = info.ModTime()
	}

	return l, nil
}

func NewLogAsync(path string, baseOffset int) (*Log, error) {
	l, err := newLog(path, baseOffset, 4096*2, false, false)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func NewLogMediumDurable(path string, baseOffset int) (*Log, error) {
	l, err := newLog(path, baseOffset, 4096, true, false)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func NewLogFullDurable(path string, baseOffset int) (*Log, error) {
	l, err := newLog(path, baseOffset, 4096, true, true)
	if err != nil {
		return nil, err
	}

	return l, nil
}

// Append adds a new record to the log.
func (l *Log) Append(payload []byte) error {
	if l.readOnly {
		return errors.New("cannot append record when lo is opended in read only mode")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	offset := uint64(l.baseOffset) + uint64(l.nextOffset) // Global offset

	header := RecordHeader{
		LogicalOffset: offset,
		PayloadSize:   uint64(len(payload)),
		Timestamp:     uint64(time.Now().UnixNano()),
	}

	buf := make([]byte, HeaderSize+len(payload))
	header.Encode(buf[:HeaderSize])
	copy(buf[HeaderSize:], payload)
	if _, err := l.writeFunc(buf); err != nil {
		return fmt.Errorf("error writing record: %w", err)
	}

	bytesWritten := HeaderSize + len(payload)
	l.nextMemoryPos += int64(bytesWritten)
	l.nextOffset += 1

	if l.nextOffset%500 != 0 {
		return nil
	}

	indexEntry := IndexEntry{
		MemoryPos:  uint32(l.nextMemoryPos),
		LogicalOff: uint32(l.nextOffset),
	}

	return l.index.WriteEntry(indexEntry)
}

func (l *Log) scanFrom(startMemoryPos int64, handleFn func(h RecordHeader, payloadPos int64) bool) error {
	err := l.flushFunc()
	if err != nil {
		return fmt.Errorf("failed to flush writer in scanFrom: %w", err)
	}

	currentPos := startMemoryPos
	for {
		var headerBuf [HeaderSize]byte

		if currentPos >= l.nextMemoryPos {
			return ErrRecordNotFoundFullScan
		}
		_, err := l.file.ReadAt(headerBuf[:], currentPos)
		if err != nil {
			return fmt.Errorf("failed read header data in scan from: %w", err)
		}

		var header RecordHeader
		header.Decode(headerBuf[:])

		payloadStartPos := currentPos + HeaderSize

		if handleFn(header, payloadStartPos) {
			return nil
		}

		currentPos += HeaderSize + int64(header.PayloadSize)
	}
}

func (l *Log) reloadNextOffset(lastEntry IndexEntry) (int64, error) {
	var lastRecordOffset uint64
	err := l.scanFrom(int64(lastEntry.MemoryPos), func(h RecordHeader, payloadPos int64) bool {
		lastRecordOffset = h.LogicalOffset
		return false
	})
	if err != nil {
		if errors.Is(err, ErrRecordNotFoundFullScan) {
			return int64(lastRecordOffset) + 1, nil
		}

		return 0, err
	}

	return int64(lastRecordOffset) + 1, nil
}

// NextOffset Public: acquires lock
// Don't use this function in internal implementation to avoid dead lock
func (l *Log) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

func (l *Log) FindRecord(targetLogicalOffset int64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	baseIndexEntry, err := l.index.FindNearest(uint32(targetLogicalOffset))
	if err != nil {
		return Record{}, err
	}

	var record Record
	var loadErr error
	err = l.scanFrom(int64(baseIndexEntry.MemoryPos), func(h RecordHeader, payloadPos int64) bool {
		if h.LogicalOffset == uint64(targetLogicalOffset) {
			record.Header = h

			payloadBytes, err := l.loadPayload(
				payloadPos,
				int64(h.PayloadSize),
			)
			if err != nil {
				loadErr = err
				return false
			}

			record.Payload = payloadBytes
			return true
		}

		if h.LogicalOffset > uint64(targetLogicalOffset) {
			return false
		}

		return false
	})

	if loadErr != nil {
		return record, fmt.Errorf("load err: %w", loadErr)
	}
	if err != nil {
		return Record{}, fmt.Errorf("failure in scanFrom: %w", err)
	}

	return record, nil
}

func (l *Log) loadPayload(payloadPos int64, payloadSize int64) ([]byte, error) {
	payloadBytes := make([]byte, payloadSize)
	_, err := l.file.ReadAt(payloadBytes, payloadPos)

	return payloadBytes, err
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	writerErr := l.closeFunc()
	indexErr := l.index.Close()
	fileErr := l.file.Close()
	return errors.Join(writerErr, indexErr, fileErr)
}
