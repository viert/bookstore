package web

import (
	"net/http"

	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/storage"
)

type roleType int

const (
	roleMaster roleType = iota
	roleSlave
)

// Server represents bookstore http server
type Server struct {
	bind        string
	storage     *storage.Storage
	role        roleType
	replicate   bool
	replicateTo string
}

// NewServer creates and configures a new Server instance
// based on a given underlying storage
func NewServer(storage *storage.Storage, cfg config.ServerCfg) *Server {
	s := &Server{
		bind:      cfg.Bind,
		storage:   storage,
		role:      roleSlave,
		replicate: false,
	}

	if cfg.IsMaster {
		s.role = roleMaster
	}

	if cfg.ReplicateTo != "" {
		s.replicate = true
		s.replicateTo = cfg.ReplicateTo
	}
	return s
}

// Start creates and configures a http server with all necessary handlers,
// then starts ListenAndServe in background and returns the server
func (s *Server) Start() *http.Server {
	srv := &http.Server{
		Addr: s.bind,
	}

	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			return
		}
	}()

	return srv
}
