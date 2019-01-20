package router

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/op/go-logging"
	"github.com/viert/bookstore/config"
)

type upstreamConfig struct {
	instanceID uint64
	name       string
	master     string
	replica    string
}

// Server represents router server object
type Server struct {
	bind       string
	upstreams  []*upstreamConfig
	panic      bool
	srv        *http.Server
	pingerStop chan bool
}

var (
	log = logging.MustGetLogger("router")
)

// NewServer creates and configures a router server with a given config
func NewServer(cfg config.RouterCfg) *Server {
	s := &Server{
		bind:       cfg.Bind,
		upstreams:  make([]*upstreamConfig, 0),
		panic:      cfg.PanicOnFaultyInstances,
		pingerStop: make(chan bool),
	}

	for name, hp := range cfg.Upstreams {
		ucfg := &upstreamConfig{
			name:       name,
			master:     hp.Master,
			replica:    hp.Replica,
			instanceID: 0,
		}
		s.upstreams = append(s.upstreams, ucfg)
	}
	return s
}

func (s *Server) checkUpstreams() error {
	return nil
}

func (s *Server) pingUpstreams() {
	<-s.pingerStop
}

// Start starts the server and background pinger
func (s *Server) Start() error {
	err := s.checkUpstreams()
	if err != nil && s.panic {
		return fmt.Errorf("panic due to upstream failure (and panic_on_faulty flag)")
	}

	r := mux.NewRouter()
	r.HandleFunc("/put", putData)
	r.HandleFunc("/get/{instanceID}/{itemID}", getData)

	s.srv = &http.Server{
		Addr:    s.bind,
		Handler: r,
	}

	go s.pingUpstreams()

	go func() {
		err := s.srv.ListenAndServe()
		if err != nil {
			return
		}
	}()

	return nil

}

// Stop stops the http server and background jobs
func (s *Server) Stop() {
	s.pingerStop <- true
	s.srv.Shutdown(nil)
}
