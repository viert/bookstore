package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/viert/bookstore/storage"

	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/web"
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

	storageFlags := os.O_RDONLY
	if cfg.IsMaster {
		storageFlags = os.O_RDWR
	}

	storageFile, err := os.OpenFile(cfg.StorageFileName, storageFlags, 0644)
	if err != nil {
		log.Fatalf("error opening storage file: %s", err)
	}

	storage, err := storage.Open(storageFile)
	if err != nil {
		log.Fatalf("error opening storage: %s", err)
	}

	srv := web.NewServer(storage, cfg).Start()
	log.Println("server started")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT)
	defer signal.Reset()

	_ = <-sigs
	srv.Shutdown(nil)

}
