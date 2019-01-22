all: bsserver bsrouter bscreate

bsserver:
	go build exe/bsserver/bsserver.go

bsrouter:
	go build exe/bsrouter/bsrouter.go

bscreate:
	go build exe/bscreate/bscreate.go

deps:
	go get -u github.com/viert/properties
	go get -u github.com/op/go-logging

clean:
	rm -f bsrouter
	rm -f bsserver