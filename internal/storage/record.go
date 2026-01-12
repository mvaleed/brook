package storage

import (
	"encoding/binary"
)

const (
	// Offset(8) + Size(8) + Timestamp(8) = 24 bytes
	HeaderSize = 24
)

type RecordHeader struct {
	LogicalOffset uint64
	PayloadSize   uint64
	Timestamp     uint64
}

type Record struct {
	Header  RecordHeader
	Payload []byte
}

type payloadRepr struct {
	startPayloadMemoryPos int64
	payloadSize           int64
}

// Encode uses stack allocation for speed
func (h *RecordHeader) Encode(dst []byte) {
	binary.BigEndian.PutUint64(dst[0:8], h.LogicalOffset)
	binary.BigEndian.PutUint64(dst[8:16], h.PayloadSize)
	binary.BigEndian.PutUint64(dst[16:24], h.Timestamp)
}

func (h *RecordHeader) Decode(src []byte) {
	h.LogicalOffset = binary.BigEndian.Uint64(src[0:8])
	h.PayloadSize = binary.BigEndian.Uint64(src[8:16])
	h.Timestamp = binary.BigEndian.Uint64(src[16:24])
}
