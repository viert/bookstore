package storage

import (
	"encoding/binary"
	"io"
)

type storeHeader struct {
	StorageID    uint64
	Version      int32
	ChunkSize    int32
	NumChunks    int32
	FreeChunkIdx int32
}

type chunkHeader struct {
	DataSize   int32
	Next       int32
	Compressed bool
	Reserved   [23]byte
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
