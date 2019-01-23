package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/viert/bookstore/common"
	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/router"
)

const (
	defaultConfigFilename = "/etc/bsrouter.cfg"
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

	cfg, err := config.ReadRouterConfig(f)
	if err != nil {
		log.Fatalf("error reading config: %s", err)
	}

	lf, err := common.ConfigureLogging(cfg.LogFileName)
	if err != nil {
		log.Fatalf("error opening logfile: %s", err)
	}
	defer lf.Close()

	srv := router.NewRouter(cfg)
	err = srv.Start()
	if err != nil {
		log.Fatalf("error starting router server: %s", err)
	}

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT)
	defer signal.Reset()

	_ = <-sigs
	srv.Stop()
}
