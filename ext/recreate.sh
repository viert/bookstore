#!/bin/bash

rm ext/example-storage*.bin
./bsctl create -s 1024 -c 1024 -i 102 -f ext/example-storage.bin
./bsctl create -s 1024 -c 1024 -i 102 -f ext/example-storage-repl.bin