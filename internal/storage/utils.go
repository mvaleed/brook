package storage

import (
	"fmt"
	"io"
	"os"
	"time"
)

// DumpFile prints all records in a file for debugging
func DumpFile(path string, head int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	headerBuf := make([]byte, HeaderSize)
	recordNum := 0

	for {
		// Read header
		_, err := io.ReadFull(f, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading header %d: %w", recordNum, err)
		}

		var h RecordHeader
		h.Decode(headerBuf)

		// Read payload
		payload := make([]byte, h.PayloadSize)
		if h.PayloadSize > 0 {
			if _, err := io.ReadFull(f, payload); err != nil {
				return fmt.Errorf("reading payload %d: %w", recordNum, err)
			}
		}

		// Print record
		fmt.Printf("Record #%d\n", recordNum)
		fmt.Printf("  Offset:    %d\n", h.LogicalOffset)
		fmt.Printf("  Size:      %d\n", h.PayloadSize)
		fmt.Printf("  Timestamp: %d (%s)\n", h.Timestamp, time.Unix(0, int64(h.Timestamp)))
		fmt.Printf("  Payload:   %q\n", truncate(payload, 100))
		fmt.Println()

		recordNum++
		if recordNum == head {
			break
		}
	}

	fmt.Printf("Total: %d records\n", recordNum)
	return nil
}

func truncate(b []byte, max int) []byte {
	if len(b) <= max {
		return b
	}
	return b[:max]
}

func TimeNowInUtc() time.Time {
	return time.Now().UTC()
}
