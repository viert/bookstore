package main

import (
	"fmt"
	"os"

	"github.com/akamensky/argparse"
)

func main() {
	parser := argparse.NewParser("bsctl", "a tool for manipulating bs storage files")

	createCmd := parser.NewCommand("create", "creates a new bs storage file")
	chunkSize := createCmd.Int("s", "size",
		&argparse.Options{Required: true, Help: "size of a single chunk data (not including chunk header)"})
	numChunks := createCmd.Int("c", "chunks",
		&argparse.Options{Required: true, Help: "total number of chunks"})
	storageFile := createCmd.File("f", "file", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644,
		&argparse.Options{Required: true, Help: "filename to create"})
	storageID := createCmd.Int("i", "stid",
		&argparse.Options{Default: 0, Help: "assign storage id (default or zero forces random storage id to be used)"})

	moveCmd := parser.NewCommand("move", "moves data from one storage to another. output storage may not be empty so it's possible to combine data from different storages into one")
	inputFile := moveCmd.File("i", "input", os.O_RDONLY, 0644,
		&argparse.Options{Required: true, Help: "input storage file"})
	outputFile := moveCmd.File("o", "output", os.O_RDWR, 0644,
		&argparse.Options{Required: true, Help: "output storage file"})

	err := parser.Parse(os.Args)

	if err != nil {
		fmt.Println(err)
	}

	if createCmd.Happened() {
		runCreate(storageFile, *chunkSize, *numChunks, *storageID)
	}

	if moveCmd.Happened() {
		runMove(inputFile, outputFile)
	}
}
