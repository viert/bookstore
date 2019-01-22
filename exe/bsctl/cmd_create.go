package main

import (
	"fmt"
	"log"
	"os"

	"github.com/viert/bookstore/storage"
)

func runCreate(f *os.File, chunkSize int, numChunks int, storageID int) {
	defer f.Close()

	if chunkSize < storage.MinChunkSize || chunkSize > storage.MaxChunkSize {
		log.Fatalf("chunk size can not be less than %d or greater than %d\n",
			storage.MinChunkSize, storage.MaxChunkSize)
	}

	if numChunks < 1 {
		log.Fatalln("number of chunks can not be less than 1")
	}

	if numChunks > storage.MaxNumChunks {
		log.Fatalf("number of chunks can not be greater than %d\n", storage.MaxNumChunks)
	}

	_, err := storage.CreateStorage(f, chunkSize, numChunks, uint64(storageID))
	if err != nil {
		log.Fatalf("error creating storage: %s", err)
	}

	r, err := os.Open(f.Name())
	if err != nil {
		log.Fatalf("error opening storage file: %s", err)
	}
	defer r.Close()

	fi, err := r.Stat()
	if err != nil {
		log.Fatalf("error getting file stat: %s", err)
	}

	st, err := storage.Open(r)
	if err != nil {
		log.Fatalf("error opening storage: %s", err)
	}
	fmt.Printf("Storage created.\nFile size:  %d bytes\nStorage ID: %d\n", fi.Size(), st.GetID())

}
