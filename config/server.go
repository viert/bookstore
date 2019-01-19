package config

import (
	"fmt"
	"io"

	"github.com/viert/properties"
)

// ServerCfg represents a server config
type ServerCfg struct {
	Bind            string
	IsMaster        bool
	ReplicateTo     string
	StorageFileName string
	StorageID       string
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

	cfg.StorageID, err = p.GetString("storage.id")
	if err != nil {
		return nil, fmt.Errorf("error reading storage.id: %s", err)
	}

	if p.KeyExists("replica.host") {
		cfg.ReplicateTo, err = p.GetString("replica.host")
		if err != nil {
			return nil, fmt.Errorf("error reading replica.host: %s", err)
		}
	}

	return cfg, nil
}
