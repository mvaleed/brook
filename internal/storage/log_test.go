package storage

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	randm "math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	// Read fills b with cryptographically secure random bytes.
	// It calls io.ReadFull internally and panics if it fails to read the full length
	// due to a system error (e.g., /dev/urandom being unavailable).
	_, err := rand.Read(b)
	if err != nil {
		return nil, fmt.Errorf("failed to read random bytes: %w", err)
	}
	return b, nil
}

func TestLog_NewLog(t *testing.T) {
	t.Run("Empty log", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)

		require.NotNil(t, log)

		require.NotNil(t, log.file)
		require.Equal(t, int64(0), log.nextMemoryPos)
		require.Equal(t, int64(0), log.nextOffset)
		require.NotNil(t, log.index)
	})

	t.Run("Close on error", func(t *testing.T) {
		// TODO: Use mock
	})

	t.Run("Non Empty Index", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)

		for i := range 1201 {
			payload := map[string]any{
				"hello": i,
				"byte":  i + 100,
			}
			payloadByte, err := json.Marshal(payload)
			require.NoError(t, err)
			err = log.Append(payloadByte)
			require.NoError(t, err)
		}
		err = log.Close()
		require.NoError(t, err)

		log, err = NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)

		require.NotEqual(t, 0, int(log.nextMemoryPos))
		require.Equal(t, 1201, int(log.nextOffset))
		require.NotNil(t, log.index)
	})
}

func TestLog_Append(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		payloadByte, err := GenerateRandomBytes(4070)
		require.NoError(t, err)
		err = log.Append(payloadByte)
		require.NoError(t, err)
		logContents, err := os.ReadFile(logPath)
		require.NoError(t, err)
		require.NotEmpty(t, logContents) // Flushed b/c medium durabability !(Not Flushed yet: as 4070 + 24(HeaderSize) < 4096)

		payloadByte, err = GenerateRandomBytes(1)
		require.NoError(t, err)
		err = log.Append(payloadByte)
		require.NoError(t, err)
		logContents, err = os.ReadFile(logPath)
		require.NoError(t, err)
		require.NotEmpty(t, logContents) // Flushed to disk as 1+4070+24+24 > 4096
	})

	t.Run("append records to test index", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		for i := range 499 {
			payloadByte, err := GenerateRandomBytes(i)
			require.NoError(t, err)

			err = log.Append(payloadByte)
			require.NoError(t, err)
		}
		logContents, err := os.ReadFile(logPath)
		require.NoError(t, err)
		require.NotEmpty(t, logContents) // Flushed to disk as 24*499 + payload_bytes > 4096

		err = log.index.Flush()
		require.NoError(t, err)

		indexContents, err := os.ReadFile(log.indexPath)
		require.NoError(t, err)
		require.Empty(t, indexContents) // records < 500

		payloadByte, err := GenerateRandomBytes(1)
		require.NoError(t, err)
		err = log.Append(payloadByte)
		require.NoError(t, err)

		err = log.index.Flush()
		require.NoError(t, err)

		indexContents, err = os.ReadFile(log.indexPath)
		require.NoError(t, err)
		require.NotEmpty(t, indexContents) // records >= 500
	})

	t.Run("Concurrent Appends to verify offsets", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		const numGoroutines = 100
		const appendsPerGoroutine = 50

		var wg sync.WaitGroup
		for range numGoroutines {
			wg.Go(func() {
				for range appendsPerGoroutine {
					err := log.Append([]byte("payload"))
					require.NoError(t, err)
				}
			})
		}
		wg.Wait()

		// Now verify invariants
		expectedRecords := numGoroutines * appendsPerGoroutine

		// Invariant 1: nextOffset should equal total appends
		require.Equal(t, expectedRecords, int(log.nextOffset))

		// Invariant 2: All offsets 0..N-1 should exist and be unique
		seen := make(map[uint64]bool)
		for i := range expectedRecords {
			record, err := log.FindRecord(int64(i))
			require.NoError(t, err)
			require.False(
				t,
				seen[record.Header.LogicalOffset],
				fmt.Sprintf("duplicate offset found: %d", record.Header.LogicalOffset),
			)
			seen[record.Header.LogicalOffset] = true
		}

		// Invariant 3: No gaps in offsets
		require.Equal(t, expectedRecords, len(seen))
	})
	t.Run("read write race", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		var wg sync.WaitGroup
		var readErrors atomic.Int64
		stop := make(chan struct{})

		for range 50 {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						err := log.Append([]byte("payload"))
						require.NoError(t, err)
					}
				}
			})
		}

		for range 50 {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						currentMax := log.NextOffset() - 1
						if currentMax < 0 {
							continue
						}

						offset := currentMax - randm.Int64N(min(currentMax+1, 10))
						record, err := log.FindRecord(offset)
						if err != nil {
							readErrors.Add(1)
							continue
						}
						if record.Header.LogicalOffset != uint64(offset) {
							readErrors.Add(1)
						}
					}
				}
			})
		}

		close(stop)
		wg.Wait()

		require.Equal(t, 0, int(readErrors.Load()), fmt.Sprintf("%d read errors during concurrent access", readErrors.Load()))
	})
}

func TestLog_FindRecord(t *testing.T) {
	t.Run("Find 1 record from 1", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		payload := map[string]any{
			"hello": 1,
		}

		payloadByte, err := json.Marshal(payload)
		require.NoError(t, err)

		err = log.Append(payloadByte)
		require.NoError(t, err)

		record, err := log.FindRecord(0)
		require.NoError(t, err)

		require.Equal(t, 0, int(record.Header.LogicalOffset))
		require.Equal(t, 11, int(record.Header.PayloadSize))
		require.Greater(t, int(record.Header.Timestamp), 0)

		require.Equal(t, payloadByte, record.Payload)
	})

	t.Run("Find 1 record from 382(without index scan)", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		storedRecords := make(map[int][]byte)

		for i := range 382 {
			payload := map[string]any{
				"hello": i,
			}

			payloadByte, errMarshal := json.Marshal(payload)
			require.NoError(t, errMarshal)

			storedRecords[i] = payloadByte

			err = log.Append(payloadByte)
			require.NoError(t, err)
		}

		record, err := log.FindRecord(181)
		require.NoError(t, err)

		require.Equal(t, 181, int(record.Header.LogicalOffset))
		require.Equal(t, 13, int(record.Header.PayloadSize))
		require.Greater(t, int(record.Header.Timestamp), 0)

		require.Equal(t, storedRecords[181], record.Payload)
	})

	t.Run("Find 1 record from 5002(with index scan)", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		storedRecords := make(map[int][]byte)

		for i := range 5002 {
			payload := map[string]any{
				"hello": i,
			}

			payloadByte, errMarshal := json.Marshal(payload)
			require.NoError(t, errMarshal)

			storedRecords[i] = payloadByte

			err = log.Append(payloadByte)
			require.NoError(t, err)
		}

		record, err := log.FindRecord(5000)
		require.NoError(t, err)

		require.Equal(t, 5000, int(record.Header.LogicalOffset))
		require.Equal(t, 14, int(record.Header.PayloadSize))
		require.Greater(t, int(record.Header.Timestamp), 0)

		require.Equal(t, storedRecords[5000], record.Payload)
	})

	t.Run("Record not found", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		storedRecords := make(map[int][]byte)

		for i := range 5002 {
			payload := map[string]any{
				"hello": i,
			}

			payloadByte, errMarshal := json.Marshal(payload)
			require.NoError(t, errMarshal)

			storedRecords[i] = payloadByte

			err = log.Append(payloadByte)
			require.NoError(t, err)
		}

		record, err := log.FindRecord(9121)
		require.Error(t, err)
		require.Equal(t, Record{}, record)
	})

	t.Run("Large payload", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "test.log")
		log, err := NewLogMediumDurable(logPath, 0)
		require.NoError(t, err)
		defer log.Close()

		payload := make(map[string]any)
		for i := range 9211 {
			key := fmt.Sprintf("hello %d", i)
			payload[key] = i * 9211
		}

		payloadByte, errMarshal := json.Marshal(payload)
		require.NoError(t, errMarshal)

		err = log.Append(payloadByte)
		require.NoError(t, err)

		record, err := log.FindRecord(0)
		require.NoError(t, err)

		require.Equal(t, payloadByte, record.Payload)
		require.Equal(t, 200322, int(record.Header.PayloadSize))
	})
}
