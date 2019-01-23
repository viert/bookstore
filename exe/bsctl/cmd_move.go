package main

import (
	"log"
	"os"

	"github.com/viert/bookstore/storage"
)

func runMove(input *os.File, output *os.File) {
	ist, err := storage.Open(input)
	if err != nil {
		log.Fatalf("error opening input storage: %s", err)
	}

	ost, err := storage.Open(output)
	if err != nil {
		log.Fatalf("error opening output storage: %s", err)
	}

	err = ist.Iter(func(idx int, data []byte) error {
		_, werr := ost.Write(data, storage.NopReplicationCallback)
		return werr
	})

	if err != nil {
		log.Fatalf("error copying data: %s", err)
	}
}
