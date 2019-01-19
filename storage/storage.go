package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"
)

const (
	// MinChunkSize holds the minimum size of a data chunk (excluding header)
	MinChunkSize = 1024
	// MaxChunkSize holds the maximum size of a data chunk (excluding header)
	MaxChunkSize = 65536
	// MaxNumChunks holds the maximum number of chunks (~132Gb for 1024k-chunk)
	MaxNumChunks = 0x8000000

	storageVersion = 1
)

// Storage is the main type representing the bookstore storage
type Storage struct {
	backend Backend
	header  storeHeader
	locker  sync.RWMutex
}

// ReplicationCallback represents a function type for
// replication mechanics. This is made as a callback because
// the local commit must depend on the result of replication
type ReplicationCallback func(idx int) error

// Open initializes a Storage instance from a given backend (typically a rw-opened file)
func Open(backend Backend) (*Storage, error) {
	s := new(Storage)
	s.backend = backend
	err := s.readHeader()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) readHeader() error {

	s.locker.RLock()
	defer s.locker.RUnlock()

	p := make([]byte, storeHeaderSize)
	_, err := s.backend.ReadAt(p, 0)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(p)
	err = binary.Read(buf, binaryLayout, &s.header)
	if err != nil {
		return err
	}

	if s.header.Version != storageVersion {
		return fmt.Errorf("storage version mismatch: file version is %d, software version is %d",
			s.header.Version, storageVersion)
	}

	return nil
}

func (s *Storage) writeHeader() error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, &s.header)
	_, err := s.backend.WriteAt(buf.Bytes(), 0)
	return err
}

func (s *Storage) getChunkPosition(idx int) int {
	if idx >= int(s.header.NumChunks) || idx < 0 {
		return -1
	}
	return storeHeaderSize + idx*int(s.header.ChunkSize)

}

func zip(data []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	defer zw.Close()
	_, err := zw.Write(data)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

func unzip(data *bytes.Buffer) ([]byte, error) {
	zr, err := gzip.NewReader(data)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out, err := ioutil.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Storage) writeTo(buf *bytes.Buffer, idx int, callback ReplicationCallback) (int, error) {
	var header chunkHeader
	var headerBuffer bytes.Buffer
	var bytesToWrite int
	var err error

	freeChunk := idx
	maxChunkDataSize := int(s.header.ChunkSize) - chunkHeaderSize
	bytesLeft := buf.Len()

	dataBuffer := buf.Bytes()
	dataBufferIdx := 0

	for bytesLeft > 0 {
		if freeChunk >= int(s.header.NumChunks) {
			return -1, fmt.Errorf("storage is full")
		}
		pos := s.getChunkPosition(freeChunk)
		if pos < 0 {
			return -1, fmt.Errorf("index out of bounds")
		}

		if bytesLeft > maxChunkDataSize {
			header = chunkHeader{
				DataSize: int32(maxChunkDataSize),
				Next:     int32(freeChunk + 1),
			}
			bytesToWrite = maxChunkDataSize
		} else {
			header = chunkHeader{
				DataSize: int32(bytesLeft),
				Next:     -1,
			}
			bytesToWrite = bytesLeft
		}
		bytesLeft -= bytesToWrite

		// preparing header buffer
		headerBuffer.Reset()
		binary.Write(&headerBuffer, binaryLayout, &header)

		// writing header buffer contents at proper position in backend
		_, err = s.backend.WriteAt(headerBuffer.Bytes(), int64(pos))
		if err != nil {
			return -1, err
		}

		// writing bytesToWrite bytes of actual data right after the header
		_, err = s.backend.WriteAt(dataBuffer[dataBufferIdx:dataBufferIdx+bytesToWrite],
			int64(pos+chunkHeaderSize))
		if err != nil {
			return -1, err
		}

		dataBufferIdx += bytesToWrite
		freeChunk++
	}

	if callback != nil {
		err = callback(idx)
		// replication is kinda atomic. so if it fails, local write
		// must fail as well
		if err != nil {
			return -1, fmt.Errorf("replication error: %s", err)
		}
	}

	s.header.FreeChunkIdx = int32(freeChunk)
	err = s.writeHeader()
	if err != nil {
		return -1, err
	}

	return idx, nil

}

// WriteTo writes data into chunks starting from given idx
func (s *Storage) WriteTo(data []byte, idx int) (int, error) {
	buf, err := zip(data)
	if err != nil {
		return -1, err
	}
	s.locker.Lock()
	defer s.locker.Unlock()
	return s.writeTo(buf, idx, nil)
}

// Write writes data into free chunks of storage
// and returns index of the starting chunk
func (s *Storage) Write(data []byte, callback ReplicationCallback) (int, error) {
	buf, err := zip(data)
	if err != nil {
		return -1, err
	}
	s.locker.Lock()
	defer s.locker.Unlock()
	idx := int(s.header.FreeChunkIdx)
	return s.writeTo(buf, idx, callback)
}

func (s *Storage) readRaw(idx int) (*bytes.Buffer, error) {
	var outBuffer bytes.Buffer
	var header chunkHeader
	var err error
	headerBytes := make([]byte, chunkHeaderSize)

	s.locker.RLock()
	defer s.locker.RUnlock()

	for {
		if idx >= int(s.header.FreeChunkIdx) || idx < 0 {
			return nil, fmt.Errorf("index out of bounds")
		}

		pos := s.getChunkPosition(idx)

		// reading chunk header
		_, err = s.backend.ReadAt(headerBytes, int64(pos))
		if err != nil {
			return nil, err
		}
		headerBuffer := bytes.NewBuffer(headerBytes)
		err = binary.Read(headerBuffer, binaryLayout, &header)
		if err != nil {
			return nil, err
		}

		// reading chunk data
		dataBytes := make([]byte, header.DataSize)
		_, err = s.backend.ReadAt(dataBytes, int64(pos+chunkHeaderSize))
		if err != nil {
			return nil, err
		}
		_, err = outBuffer.Write(dataBytes)
		if err != nil {
			return nil, err
		}

		if header.Next < 0 {
			break
		}
		idx = int(header.Next)
	}

	return &outBuffer, nil
}

func (s *Storage) Read(idx int) ([]byte, error) {
	buf, err := s.readRaw(idx)
	if err != nil {
		return nil, err
	}
	return unzip(buf)
}

// GetID returns storage ID from storage file header
func (s *Storage) GetID() uint64 {
	return s.header.StorageID
}
