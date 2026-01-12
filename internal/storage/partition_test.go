package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogName_newLogNameFromInt(t *testing.T) {
	t.Run("single digit", func(t *testing.T) {
		output := newLogNameFromInt(0)
		require.Equal(t, logName("000000000000000.log"), output)

		output = newLogNameFromInt(3)
		require.Equal(t, logName("000000000000003.log"), output)

		output = newLogNameFromInt(9)
		require.Equal(t, logName("000000000000009.log"), output)
	})

	t.Run("double digit", func(t *testing.T) {
		output := newLogNameFromInt(12)
		require.Equal(t, logName("000000000000012.log"), output)

		output = newLogNameFromInt(35)
		require.Equal(t, logName("000000000000035.log"), output)

		output = newLogNameFromInt(91)
		require.Equal(t, logName("000000000000091.log"), output)
	})

	t.Run("many digits", func(t *testing.T) {
		output := newLogNameFromInt(1212)
		require.Equal(t, logName("000000000001212.log"), output)

		output = newLogNameFromInt(20000)
		require.Equal(t, logName("000000000020000.log"), output)

		output = newLogNameFromInt(123456789)
		require.Equal(t, logName("000000123456789.log"), output)
	})
}

func TestPartition_NewPartition(t *testing.T) {
	t.Run("empty partition dir", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/1/")

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.Equal(t, p.dir, partitionDir)
		require.Len(t, p.segments, 1)
		require.Equal(t, 0, p.segments[0].BaseOffset)
		require.Equal(t, filepath.Join(partitionDir, "000000000000000.log"), p.segments[0].Path)

		require.NotNil(t, p.activeLog)
		require.Equal(t, "000000000000000.log", p.activeLogName.string())
		require.Equal(t, 0, p.nextOffset)
	})
	t.Run("with 1 log already existing", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/1/")
		err := os.MkdirAll(partitionDir, 0o755)
		require.NoError(t, err)
		_, err = NewLogMediumDurable(filepath.Join(partitionDir, newLogNameFromInt(0).string()), 0)
		require.NoError(t, err)

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.Equal(t, p.dir, partitionDir)
		require.Len(t, p.segments, 1)
		require.Equal(t, 0, p.segments[0].BaseOffset)
		require.Equal(t, filepath.Join(partitionDir, "000000000000000.log"), p.segments[0].Path)

		require.NotNil(t, p.activeLog)
		require.Equal(t, "000000000000000.log", p.activeLogName.string())
		require.Equal(t, 0, p.nextOffset)
	})
	t.Run("with more than 1 log already existing", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/1/")
		err := os.MkdirAll(partitionDir, 0o755)
		require.NoError(t, err)
		logPath1 := filepath.Join(partitionDir, newLogNameFromInt(0).string())
		_, err = NewLogMediumDurable(logPath1, 0)
		require.NoError(t, err)

		logPath2 := filepath.Join(partitionDir, newLogNameFromInt(101).string())
		_, err = NewLogMediumDurable(logPath2, 0)
		require.NoError(t, err)

		logPath3 := filepath.Join(partitionDir, newLogNameFromInt(201).string())
		_, err = NewLogMediumDurable(logPath3, 0)
		require.NoError(t, err)

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		require.Equal(t, p.dir, partitionDir)
		require.Equal(t, 3, len(p.segments))

		require.Equal(t, p.segments[0].BaseOffset, 0)
		require.Equal(t, p.segments[0].Path, logPath1)

		require.Equal(t, p.segments[1].BaseOffset, 101)
		require.Equal(t, p.segments[1].Path, logPath2)

		require.Equal(t, p.segments[2].BaseOffset, 201)
		require.Equal(t, p.segments[2].Path, logPath3)

		require.NotNil(t, p.activeLog)
		require.Equal(t, "000000000000201.log", p.activeLogName.string())
		require.Equal(t, 201, p.nextOffset)
	})
}

func TestPartition_Append(t *testing.T) {
	t.Run("append 1 record", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/")

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		data, err := GenerateRandomBytes(100)
		require.NoError(t, err)

		err = p.Append(data)
		require.NoError(t, err)

		require.Equal(t, "000000000000000.log", p.activeLogName.string())
		record, err := p.activeLog.FindRecord(0)
		require.NoError(t, err)

		require.Equal(t, 100, int(record.Header.PayloadSize))
	})
	t.Run("append limit records", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/")

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		for i := range 10000 {
			data, err := GenerateRandomBytes(i + 1 + 100)
			require.NoError(t, err)

			err = p.Append(data)
			require.NoError(t, err)

			require.Equal(t, "000000000000000.log", p.activeLogName.string())
		}
		require.NoError(t, err)

		err = p.Append([]byte("payload"))
		require.NoError(t, err)

		require.Equal(t, "000000000010001.log", p.activeLogName.string())
		require.Len(t, p.segments, 2)
		require.Equal(t, 0, p.segments[0].BaseOffset)
		require.Equal(t, 10001, p.segments[1].BaseOffset)

		record, err := p.activeLog.FindRecord(10001)
		require.NoError(t, err)

		require.Equal(t, 7, int(record.Header.PayloadSize))
		require.Equal(t, "payload", string(record.Payload))
	})
}

func TestPartition_Read(t *testing.T) {
	t.Run("basic single log read", func(t *testing.T) {
		partitionDir := filepath.Join(t.TempDir(), "partition/")

		p, err := NewPartition(partitionDir)
		require.NoError(t, err)
		require.NotNil(t, p)

		data1 := []byte("hello 1")
		err = p.Append(data1)
		require.NoError(t, err)

		data2 := []byte("hello 2")

		err = p.Append(data2)
		require.NoError(t, err)

		record, err := p.Read(0)
		require.NoError(t, err)
		require.Equal(t, data1, record.Payload)

		record, err = p.Read(1)
		require.NoError(t, err)
		require.Equal(t, data2, record.Payload)
	})
	// TODO: add more complicated tests
}
