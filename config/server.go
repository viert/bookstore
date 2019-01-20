package config

import (
	"fmt"
	"io"
	"time"

	"github.com/viert/properties"
)

const (
	defaultReplicationTimeout = 250 // ms
	defaultLogFileName        = "/var/log/bookstore.log"
)

// ServerCfg represents a server config
type ServerCfg struct {
	Bind               string
	IsMaster           bool
	ReplicateTo        string
	ReplicationTimeout time.Duration
	StorageFileName    string
	LogFileName        string
}

type HostPair struct {
	Master  string
	Replica string
}

// RouterCfg represents a router config
type RouterCfg struct {
	Bind                   string
	LogFileName            string
	PanicOnFaultyInstances bool
	Upstreams              map[string]HostPair
}

// ReadServerConfig reads and returns a bookstore config
// from an io.Reader object
func ReadServerConfig(r io.Reader) (*ServerCfg, error) {
	p, err := properties.Read(r)
	if err != nil {
		return nil, err
	}

	cfg := &ServerCfg{}

	cfg.Bind, err = p.GetString("main.bind")
	if err != nil {
		return nil, fmt.Errorf("error reading main.bind: %s", err)
	}

	cfg.IsMaster, err = p.GetBool("main.master")
	if err != nil {
		return nil, fmt.Errorf("error reading main.master: %s", err)
	}

	cfg.StorageFileName, err = p.GetString("storage.file")
	if err != nil {
		return nil, fmt.Errorf("error reading storage.file: %s", err)
	}

	if p.KeyExists("replica.host") {
		cfg.ReplicateTo, err = p.GetString("replica.host")
		if err != nil {
			return nil, fmt.Errorf("error reading replica.host: %s", err)
		}

		timeout, err := p.GetInt("replica.timeout")
		if err != nil {
			timeout = defaultReplicationTimeout
		}
		cfg.ReplicationTimeout = time.Duration(timeout) * time.Millisecond
	}

	cfg.LogFileName, err = p.GetString("main.log")
	if err != nil {
		cfg.LogFileName = defaultLogFileName
	}

	return cfg, nil
}
