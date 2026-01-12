package storage

import "encoding/binary"

const (
	offWidth   = 4
	posWidth   = 4
	entryWidth = offWidth + posWidth // Total: 8 bytes
)

// IndexEntry represents the logical data, decoupled from bytes.
type IndexEntry struct {
	LogicalOff uint32 // The "Message Number" (Logical)
	MemoryPos  uint32 // The "Byte Location" (Physical)
}

// Marshal writes an entry into a byte slice.
// This ensures the Writer writes exactly what the Reader expects.
func (ie IndexEntry) Marshal(dst []byte) {
	binary.BigEndian.PutUint32(dst[0:offWidth], ie.LogicalOff)
	binary.BigEndian.PutUint32(dst[offWidth:entryWidth], ie.MemoryPos)
}

// Unmarshal reads an entry from a byte slice.
// This ensures the Reader reads exactly what the Writer wrote.
// Note: We avoid bounds checking here for speed; the caller must check.
func (ie *IndexEntry) Unmarshal(src []byte) {
	ie.LogicalOff = binary.BigEndian.Uint32(src[0:offWidth])
	ie.MemoryPos = binary.BigEndian.Uint32(src[offWidth:entryWidth])
}
