package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type logName string

func newLogNameFromInt(number int) logName {
	numberStr := fmt.Sprintf("%d", number)
	fill := 15 // TODO: think about this
	if len(numberStr) > fill {
		panic("todo: handle the scenario where logname is greater than fill size. look into newLogNameFromInt")
	}
	s := ""
	for range fill - len(numberStr) {
		s += "0"
	}
	s = s + numberStr + ".log"
	return logName(s)
}

func newLogNameFromString(ln string) logName {
	return logName(ln)
}

func (ln logName) toInt() int {
	lnSplit := strings.Split(string(ln), ".")[0]
	lnInt, err := strconv.Atoi(lnSplit)
	if err != nil {
		panic("wtf happened to log name? check ln.toInt()")
	}

	return lnInt
}

func (ln logName) string() string {
	return string(ln)
}

type Segment struct {
	BaseOffset int
	Path       string
}

type Partition struct {
	mu            sync.RWMutex
	dir           string
	segments      []Segment
	activeLog     *Log
	activeLogName logName
	nextOffset    int
}

func NewPartition(dir string) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logs, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var activeLogName logName
	segments := make([]Segment, 0)

	if len(logs) == 0 {
		activeLogName = newLogNameFromInt(0)
		segments = append(segments, Segment{
			BaseOffset: activeLogName.toInt(),
			Path:       filepath.Join(dir, activeLogName.string()),
		})
	} else {
		logNames := make([]logName, 0)
		for _, entry := range logs {
			if !(strings.HasSuffix(entry.Name(), ".log")) {
				continue
			}

			ln := newLogNameFromString(entry.Name())

			logNames = append(logNames, ln)
			segments = append(segments, Segment{
				BaseOffset: ln.toInt(),
				Path:       filepath.Join(dir, ln.string()),
			})
		}

		activeLogName = logNames[len(logNames)-1]
	}

	baseOffsetForActiveLog := activeLogName.toInt()
	activeLog, err := NewLogMediumDurable(filepath.Join(dir, activeLogName.string()), baseOffsetForActiveLog)
	if err != nil {
		return nil, err
	}

	nextOffset := baseOffsetForActiveLog + int(activeLog.nextOffset)

	p := &Partition{
		dir:           dir,
		activeLog:     activeLog,
		nextOffset:    nextOffset,
		activeLogName: activeLogName,
		segments:      segments,
	}
	return p, nil
}

func (p *Partition) rotate() error {
	if time.Since(p.activeLog.createdAt) > 24*time.Hour ||
		p.activeLog.NextOffset() >= 10000 { // TODO: think about this
		err := p.activeLog.Close()
		if err != nil {
			return fmt.Errorf("error while closing active log: %w", err)
		}
		p.activeLogName = newLogNameFromInt(p.nextOffset + 1)
		baseOffsetForActiveLog := p.activeLogName.toInt()
		newLogPath := filepath.Join(p.dir, p.activeLogName.string())

		p.activeLog, err = NewLogMediumDurable(newLogPath, baseOffsetForActiveLog)
		if err != nil {
			return fmt.Errorf("error while createing new active log: %w", err)
		}
		p.segments = append(p.segments, Segment{
			BaseOffset: baseOffsetForActiveLog,
			Path:       newLogPath,
		})
	}
	return nil
}

func (p *Partition) Append(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.rotate()
	if err != nil {
		return fmt.Errorf("error appending new record to partition because rotation failed: %w", err)
	}

	err = p.activeLog.Append(data)
	if err != nil {
		return fmt.Errorf("error appending new record: %w", err)
	}

	p.nextOffset += 1
	return nil
}

func (p *Partition) Read(offset int) (Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	nearestSegmentIdx := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].BaseOffset > offset
	})
	nearestSegmentIdx = max(nearestSegmentIdx-1, 0)

	nearestSegment := p.segments[nearestSegmentIdx]

	l, err := NewLogReadOnly(nearestSegment.Path, nearestSegment.BaseOffset) // Cache this or smth
	if err != nil {
		return Record{}, fmt.Errorf("unable to open log segment in read only: %w", err)
	}
	defer l.Close()

	return l.FindRecord(int64(offset))
}
