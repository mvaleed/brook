package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_NewIndex(t *testing.T) {
	t.Run("creates index with valid file", func(t *testing.T) {
		indexPath := filepath.Join(t.TempDir(), "test.index")

		index, err := NewIndex(indexPath)
		require.NoError(t, err)
		defer index.Close()

		assert.FileExists(t, indexPath)
	})

	t.Run("truncates to valid entry boundary", func(t *testing.T) {
		testCases := []struct {
			name         string
			inputSize    int
			expectedSize int
		}{
			{"9 bytes truncates to 8", 9, 8},
			{"10 bytes truncates to 8", 10, 8},
			{"15 bytes truncates to 8", 15, 8},
			{"16 bytes stays 16", 16, 16},
			{"7 bytes truncates to 0", 7, 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				indexPath := filepath.Join(t.TempDir(), "test.index")

				data := make([]byte, tc.inputSize)
				err := os.WriteFile(indexPath, data, 0o644)
				require.NoError(t, err)

				index, err := NewIndex(indexPath)
				require.NoError(t, err)
				index.Close()

				info, err := os.Stat(indexPath)
				require.NoError(t, err)
				assert.Equal(t, int64(tc.expectedSize), info.Size())
			})
		}
	})

	t.Run("cleans up on truncate failure", func(t *testing.T) {
		indexPath := filepath.Join(t.TempDir(), "test.index")

		err := os.WriteFile(indexPath, make([]byte, 5), 0o444)
		require.NoError(t, err)
		defer os.Chmod(indexPath, 0o644)

		index, err := NewIndex(indexPath)

		assert.Error(t, err)
		assert.Nil(t, index)
	})
}

func TestIndex_WriteEntry(t *testing.T) {
	t.Run("buffers writes until close and content correct", func(t *testing.T) {
		indexPath := filepath.Join(t.TempDir(), "test.index")
		index, err := NewIndex(indexPath)
		require.NoError(t, err)

		entries := []IndexEntry{
			{LogicalOff: 1, MemoryPos: 100},
			{LogicalOff: 2, MemoryPos: 200},
		}
		for _, entry := range entries {
			err = index.WriteEntry(entry)
			require.NoError(t, err)
		}

		// File should be empty (buffered)
		contents, err := os.ReadFile(indexPath)
		require.NoError(t, err)
		assert.Empty(t, contents)

		err = index.Close()
		require.NoError(t, err)

		// Verify written content
		contents, err = os.ReadFile(indexPath)
		require.NoError(t, err)

		expected := make([]byte, len(entries)*entryWidth)
		for i, e := range entries {
			e.Marshal(expected[i*entryWidth:])
		}
		assert.Equal(t, expected, contents)
	})

	t.Run("flushes when buffer is full", func(t *testing.T) {
		indexPath := filepath.Join(t.TempDir(), "test.index")
		index, err := NewIndex(indexPath)
		require.NoError(t, err)
		defer index.Close()

		entriesPerBuffer := index.writerBufferSize / entryWidth

		// Fill buffer exactly
		for i := range entriesPerBuffer {
			err = index.WriteEntry(IndexEntry{
				LogicalOff: uint32(i),
				MemoryPos:  uint32(i * 8),
			})
			require.NoError(t, err)
		}

		// Buffer full but not yet flushed
		contents, err := os.ReadFile(indexPath)
		require.NoError(t, err)
		assert.Empty(t, contents)

		// One more entry should trigger flush
		err = index.WriteEntry(IndexEntry{LogicalOff: 9999, MemoryPos: 9999})
		require.NoError(t, err)

		contents, err = os.ReadFile(indexPath)
		require.NoError(t, err)
		assert.NotEmpty(t, contents, "buffer should have flushed")
	})
}
