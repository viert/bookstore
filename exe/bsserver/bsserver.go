package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/viert/bookstore/common"
	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/server"
	"github.com/viert/bookstore/storage"
)

const (
	defaultConfigFilename = "/etc/bsserver.cfg"
)

func main() {
	var configFilename string
	flag.StringVar(&configFilename, "c", "", "configuration filename")
	flag.Parse()

	if configFilename == "" {
		configFilename = defaultConfigFilename
	}

	f, err := os.Open(configFilename)
	if err != nil {
		log.Fatalf("can not open config file %s: %s", configFilename, err)
	}
	defer f.Close()

	cfg, err := config.ReadServerConfig(f)
	if err != nil {
		log.Fatalf("error reading config: %s", err)
	}

	lf, err := common.ConfigureLogging(cfg.LogFileName)
	if err != nil {
		log.Fatalf("error opening logfile: %s", err)
	}
	defer lf.Close()

	storageFile, err := os.OpenFile(cfg.StorageFileName, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("error opening storage file: %s", err)
	}

	storage, err := storage.Open(storageFile)
	if err != nil {
		log.Fatalf("error opening storage: %s", err)
	}

	srv, err := server.NewServer(storage, cfg).Start()
	if err != nil {
		log.Fatalf("error starting server: %s", err)
	}

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT)
	defer signal.Reset()

	_ = <-sigs
	srv.Shutdown(nil)

}
