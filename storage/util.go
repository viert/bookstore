package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"time"
)

// CreateStorage creates and initializes binary structure
// of a storage using any io.Writer
func CreateStorage(w io.Writer, chunkDataSize int, numChunks int, storageID uint64) (uint64, error) {
	if storageID == 0 {
		rand.Seed(time.Now().UnixNano())
		storageID = rand.Uint64()
	}

	header := storeHeader{
		StorageID:    storageID,
		Version:      storageVersion,
		ChunkSize:    int32(chunkDataSize + chunkHeaderSize),
		NumChunks:    int32(numChunks),
		FreeChunkIdx: 0,
	}

	err := binary.Write(w, binaryLayout, header)
	if err != nil {
		return 0, fmt.Errorf("error writing header: %s", err)
	}

	cHeader := new(chunkHeader)
	cHeader.Next = -1
	cData := make([]byte, chunkDataSize)

	for i := 0; i < numChunks; i++ {
		err := binary.Write(w, binaryLayout, cHeader)
		if err != nil {
			return 0, fmt.Errorf("error writing chunk header: %s", err)
		}

		_, err = w.Write(cData)
		if err != nil {
			return 0, fmt.Errorf("error writing chunk data space: %s", err)
		}
	}

	return storageID, nil
}
