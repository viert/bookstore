package config

import (
	"fmt"
	"io"
	"time"

	"github.com/viert/properties"
)

const (
	defaultPanic                = false
	defaultStorageTimeout       = 500 // ms
	defaultStorageCheckInterval = 30  // sec
)

type HostPair struct {
	Master  string
	Replica string
}

// RouterCfg represents a router config
type RouterCfg struct {
	Bind                   string
	LogFileName            string
	PanicOnFaultyInstances bool
	StorageTimeout         time.Duration
	StorageCheckInterval   time.Duration
	Upstreams              map[string]HostPair
}

// ReadRouterConfig reads and returns a bookstore router config
// from an io.Reader object
func ReadRouterConfig(r io.Reader) (*RouterCfg, error) {
	p, err := properties.Read(r)
	if err != nil {
		return nil, err
	}

	cfg := &RouterCfg{}

	cfg.Bind, err = p.GetString("main.bind")
	if err != nil {
		return nil, fmt.Errorf("error reading main.bind: %s", err)
	}

	cfg.LogFileName, err = p.GetString("main.log")
	if err != nil {
		cfg.LogFileName = ""
	}

	timeout, err := p.GetInt("main.storage_timeout")
	if err != nil {
		timeout = defaultStorageTimeout
	}
	cfg.StorageTimeout = time.Duration(timeout) * time.Millisecond

	checkInterval, err := p.GetInt("main.storage_check_interval")
	if err != nil {
		checkInterval = defaultStorageCheckInterval
	}
	cfg.StorageCheckInterval = time.Duration(checkInterval) * time.Second

	cfg.PanicOnFaultyInstances, err = p.GetBool("main.panic_on_faulty")
	if err != nil {
		cfg.PanicOnFaultyInstances = defaultPanic
	}

	cfg.Upstreams = make(map[string]HostPair)

	subkeys, err := p.Subkeys("")
	if err != nil {
		return nil, fmt.Errorf("error reading properties subkeys: %s", err)
	}

	for _, key := range subkeys {
		if key == "main" {
			continue
		}

		hp := HostPair{}
		hp.Master, _ = p.GetString(key + ".master")
		hp.Replica, _ = p.GetString(key + ".replica")

		if hp.Master != "" || hp.Replica != "" {
			cfg.Upstreams[key] = hp
		}
	}

	return cfg, nil
}
