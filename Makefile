BSSERVER_SRC =  exe/bsserver/bsserver.go \
				server/*.go \
				storage/*.go \
				config/server.go \
				common/*.go

BSROUTER_SRC =	exe/bsrouter/bsrouter.go \
				router/*.go \
				storage/*.go \
				config/router.go \
				common/*.go

BSCTL_SRC = exe/bsctl/*.go \
			storage/*.go

all: bsserver bsrouter bsctl

bsserver: $(BSSERVER_SRC)
	go build exe/bsserver/bsserver.go

bsrouter: $(BSROUTER_SRC)
	go build exe/bsrouter/bsrouter.go

bsctl: $(BSCTL_SRC)
	go build exe/bsctl/*.go

deps:
	go get -u github.com/viert/properties
	go get -u github.com/op/go-logging
	go get -u github.com/akamensky/argparse

clean:
	rm -f bsrouter
	rm -f bsserver
	rm -f bsctl