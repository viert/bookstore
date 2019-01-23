package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"

	logging "github.com/op/go-logging"
)

const (
	// MinChunkSize holds the minimum size of a data chunk (excluding header)
	MinChunkSize = 64
	// MaxChunkSize holds the maximum size of a data chunk (excluding header)
	MaxChunkSize = 65536
	// MaxNumChunks holds the maximum number of chunks (~132Gb for 1024k-chunk)
	MaxNumChunks = 0x8000000

	storageVersion = 1
)

var (
	log = logging.MustGetLogger("bookstore")
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

// IterationCallback is called with every item in storage
// when using Iter() method
type IterationCallback func(idx int, data []byte) error

// NopReplicationCallback is a replication callback doing nothing
func NopReplicationCallback(idx int) error {
	return nil
}

// Open initializes a Storage instance from a given backend (typically a rw-opened file)
func Open(backend Backend) (*Storage, error) {
	s := new(Storage)
	s.backend = backend
	err := s.readHeader()
	if err != nil {
		log.Errorf("error reading storage header: %s", err)
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

func (s *Storage) writeTo(buf *bytes.Buffer, idx int, callback ReplicationCallback, gzipped bool) (int, error) {
	var header chunkHeader
	var headerBuffer bytes.Buffer
	var bytesToWrite int
	var err error

	currChunk := idx
	maxChunkDataSize := s.GetChunkDataSize()
	bytesLeft := buf.Len()

	dataBuffer := buf.Bytes()
	dataBufferIdx := 0

	for bytesLeft > 0 {
		log.Debugf("current chunk idx=%d", currChunk)
		if currChunk >= int(s.header.NumChunks) {
			return -1, fmt.Errorf("storage is full")
		}
		pos := s.getChunkPosition(currChunk)
		if pos < 0 {
			return -1, fmt.Errorf("index out of bounds")
		}

		if bytesLeft > maxChunkDataSize {
			log.Debugf("Data size (%d) is greater than max chunk data size (%d)",
				bytesLeft, maxChunkDataSize)
			header = chunkHeader{
				DataSize:   int32(maxChunkDataSize),
				Next:       int32(currChunk + 1),
				Compressed: gzipped,
			}
			bytesToWrite = maxChunkDataSize
		} else {
			header = chunkHeader{
				DataSize:   int32(bytesLeft),
				Next:       -1,
				Compressed: gzipped,
			}
			bytesToWrite = bytesLeft
		}
		bytesLeft -= bytesToWrite

		// preparing header buffer
		headerBuffer.Reset()
		binary.Write(&headerBuffer, binaryLayout, &header)

		// writing header buffer contents at proper position in backend
		n, err := s.backend.WriteAt(headerBuffer.Bytes(), int64(pos))
		if err != nil {
			return -1, err
		}
		log.Debugf("wrote %d bytes of chunk header at %d", n, pos)

		// writing bytesToWrite bytes of actual data right after the header
		n, err = s.backend.WriteAt(dataBuffer[dataBufferIdx:dataBufferIdx+bytesToWrite],
			int64(pos+chunkHeaderSize))
		if err != nil {
			return -1, err
		}
		log.Debugf("wrote %d bytes of data at %d", n, pos)

		dataBufferIdx += bytesToWrite
		currChunk++
	}

	if callback != nil {
		err = callback(idx)
		// replication is kinda atomic. so if it fails, local write
		// must fail as well
		if err != nil {
			return -1, fmt.Errorf("replication error: %s", err)
		}
	}

	s.header.FreeChunkIdx = int32(currChunk)
	err = s.writeHeader()
	if err != nil {
		log.Errorf("error writing storage header: %s", err)
		return -1, err
	}

	return idx, nil

}

// WriteTo writes data into chunks starting from given idx
func (s *Storage) WriteTo(data []byte, idx int, callback ReplicationCallback) (int, error) {
	plainDataLength := len(data)
	log.Debugf("data size is %d", plainDataLength)
	buf, err := zip(data)
	gzipped := true
	if err != nil {
		log.Errorf("error compressing data: %s", err)
		return -1, err
	}
	log.Debugf("compressed data size is %d", buf.Len())

	if plainDataLength < buf.Len() {
		log.Debug("about to write uncompressed data")
		buf = bytes.NewBuffer(data)
		gzipped = false
	}
	s.locker.Lock()
	defer s.locker.Unlock()
	if idx < 0 {
		idx = int(s.header.FreeChunkIdx)
	}

	idx, err = s.writeTo(buf, idx, callback, gzipped)
	if err != nil {
		log.Errorf("error writing data to storage: %s", err)
	}
	return idx, err
}

// Write writes data into free chunks of storage
// and returns index of the starting chunk
func (s *Storage) Write(data []byte, callback ReplicationCallback) (int, error) {
	return s.WriteTo(data, -1, callback)
}

func (s *Storage) readRaw(idx int) (*bytes.Buffer, int, bool, error) {
	var outBuffer bytes.Buffer
	var header chunkHeader
	var err error
	var chunkCount = 0
	headerBytes := make([]byte, chunkHeaderSize)

	s.locker.RLock()
	defer s.locker.RUnlock()

	for {
		chunkCount++
		if idx >= int(s.header.FreeChunkIdx) || idx < 0 {
			return nil, 0, false, fmt.Errorf("index out of bounds")
		}

		pos := s.getChunkPosition(idx)

		// reading chunk header
		_, err = s.backend.ReadAt(headerBytes, int64(pos))
		if err != nil {
			return nil, 0, false, err
		}
		headerBuffer := bytes.NewBuffer(headerBytes)
		err = binary.Read(headerBuffer, binaryLayout, &header)
		if err != nil {
			return nil, 0, false, err
		}

		// reading chunk data
		dataBytes := make([]byte, header.DataSize)
		_, err = s.backend.ReadAt(dataBytes, int64(pos+chunkHeaderSize))
		if err != nil {
			return nil, 0, false, err
		}
		_, err = outBuffer.Write(dataBytes)
		if err != nil {
			return nil, 0, false, err
		}

		if header.Next < 0 {
			break
		}
		idx = int(header.Next)
	}

	return &outBuffer, chunkCount, header.Compressed, nil
}

func (s *Storage) Read(idx int) ([]byte, error) {
	log.Debugf("reading item %d", idx)
	buf, _, gzipped, err := s.readRaw(idx)
	if err != nil {
		return nil, err
	}

	if gzipped {
		log.Debugf("uncompressing item %d", idx)
		return unzip(buf)
	}
	log.Debugf("item %d is not compressed", idx)
	return buf.Bytes(), nil
}

// GetID returns storage ID from storage file header
func (s *Storage) GetID() uint64 {
	return s.header.StorageID
}

// GetChunkSize returns chunk size from storage file header
func (s *Storage) GetChunkSize() int {
	return int(s.header.ChunkSize)
}

// GetNumChunks returns total number of chunks from storage file header
func (s *Storage) GetNumChunks() int {
	return int(s.header.NumChunks)
}

// GetChunkDataSize returns actual data size of one chunk
func (s *Storage) GetChunkDataSize() int {
	return int(s.header.ChunkSize) - chunkHeaderSize
}

// IsFull returns whether or not the storage is full
func (s *Storage) IsFull() bool {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.header.FreeChunkIdx >= s.header.NumChunks
}

// Iter iterates over items calling callback with each item
// it comes across
func (s *Storage) Iter(callback IterationCallback) error {
	var data []byte
	s.locker.RLock()
	defer s.locker.RUnlock()

	idx := 0
	for idx < int(s.header.FreeChunkIdx) {
		buf, length, gzipped, err := s.readRaw(idx)
		if err != nil {
			return err
		}

		if gzipped {
			data, err = unzip(buf)
			if err != nil {
				return err
			}
		} else {
			data = buf.Bytes()
		}

		err = callback(idx, data)
		if err != nil {
			return err
		}
		idx += length
	}
	return nil
}
