package main

import (
	"flag"
	"log"
	"os"

	"github.com/viert/bookstore/storage"
)

var (
	err           error
	storeFilename string
	chunkSize     int
	chunkCount    int
)

func main() {
	flag.StringVar(&storeFilename, "-f", "", "storage filename to create")
	flag.IntVar(&chunkCount, "-c", 0, "number of chunks")
	flag.IntVar(&chunkSize, "-s", 0, "chunk size")
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

	f, err := os.OpenFile(storeFilename, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("error creating file: %s", err)
	}
	defer f.Close()

	err = storage.CreateStorage(f, chunkSize, chunkCount)
	if err != nil {
		log.Fatalf("error creating storage: %s", err)
	}
}
