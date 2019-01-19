package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/viert/bookstore/storage"
)

func main() {
	var err error
	var storeFilename string
	var chunkSize int
	var chunkCount int
	var storageID uint64

	flag.StringVar(&storeFilename, "f", "", "storage filename to create")
	flag.IntVar(&chunkCount, "c", 0, "number of chunks")
	flag.IntVar(&chunkSize, "s", 0, "chunk size")
	flag.Uint64Var(&storageID, "i", 0, "assign storage id (random by default)")
	flag.Parse()

	if storeFilename == "" {
		log.Fatalln("storage filename can not be empty")
	}

	if chunkSize < storage.MinChunkSize || chunkSize > storage.MaxChunkSize {
		log.Fatalf("chunk size can not be less than %d or greater than %d\n",
			storage.MinChunkSize, storage.MaxChunkSize)
	}

	if chunkCount < 1 {
		log.Fatalln("number of chunks can not be less than 1")
	}

	if chunkCount > storage.MaxNumChunks {
		log.Fatalf("number of chunks can not be greater than %d\n", storage.MaxNumChunks)
	}

	_, err = os.Stat(storeFilename)
	if err == nil {
		log.Fatalln("file already exists")
	}

	f, err := os.OpenFile(storeFilename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("error creating file: %s", err)
	}
	defer f.Close()

	storageID, err = storage.CreateStorage(f, chunkSize, chunkCount, storageID)
	if err != nil {
		log.Fatalf("error creating storage: %s", err)
	}

	r, err := os.Open(storeFilename)
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
