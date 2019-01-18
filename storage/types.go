package storage

import (
	"encoding/binary"
	"io"
)

type storeHeader struct {
	Version      int32
	ChunkSize    int32
	NumChunks    int32
	FreeChunkIdx int32
}

type chunkHeader struct {
	DataSize int32
	Next     int32
	Reserved [24]byte
}

// Backend represents an interface of storage backend (typically a file)
type Backend interface {
	io.ReaderAt
	io.WriterAt
	io.Writer
}

var (
	storeHeaderSize = binary.Size(storeHeader{})
	chunkHeaderSize = binary.Size(chunkHeader{})
	binaryLayout    = binary.LittleEndian
)

func (h *storeHeader) isFull() bool {
	return h.FreeChunkIdx >= h.NumChunks
}
