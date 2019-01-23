package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/op/go-logging"

	"github.com/gorilla/mux"

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

	replClient *http.Client
}

var (
	log = logging.MustGetLogger("bookstore")
)

// NewServer creates and configures a new Server instance
// based on a given underlying storage
func NewServer(storage *storage.Storage, cfg *config.ServerCfg) *Server {
	s := &Server{
		bind:      cfg.Bind,
		storage:   storage,
		role:      roleSlave,
		replicate: false,
	}
	rtype := "replica"

	if cfg.IsMaster {
		s.role = roleMaster
		rtype = "master"
	}

	if cfg.ReplicateTo != "" {
		s.replicate = true
		s.replicateTo = cfg.ReplicateTo
		s.replClient = &http.Client{
			Timeout: cfg.ReplicationTimeout,
		}
	}

	log.Infof("Server configured as %s", rtype)
	return s
}

func (s *Server) checkReplication() error {
	log.Info("Checking replication...")
	resp, err := s.replClient.Get(fmt.Sprintf("%s/api/v1/info", s.replicateTo))
	if err != nil {
		return fmt.Errorf("error getting server info from replica: %s", err)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response from replica: %s", err)
	}
	var info InfoResponse
	err = json.Unmarshal(content, &info)
	if err != nil {
		return fmt.Errorf("error unmarshalling json from replica: %s", err)
	}

	if info.ServerType != "replica" {
		return fmt.Errorf("invalid server type on replica: %s", info.ServerType)
	}

	log.Infof("Local chunk data size is %d, replica chunk data size is %d",
		s.storage.GetChunkDataSize(), info.ChunkDataSize)

	if info.ChunkDataSize < s.storage.GetChunkDataSize() {
		return fmt.Errorf("insufficient chunk data size on replica")
	}

	log.Infof("Local storage has %d chunks, replica has %d",
		s.storage.GetNumChunks(), info.NumChunks)
	if info.NumChunks < s.storage.GetNumChunks() {
		return fmt.Errorf("insufficient replica storage size")
	}

	log.Infof("Local StorageID: %d", s.storage.GetID())
	log.Infof("Replica StorageID: %d", info.StorageID)
	if info.StorageID != s.storage.GetID() {
		return fmt.Errorf("master and replica's storage IDs don't match")
	}

	return nil
}

// Start creates and configures a http server with all necessary handlers,
// then starts ListenAndServe in background and returns the server
func (s *Server) Start() (*http.Server, error) {

	if s.replicate {
		err := s.checkReplication()
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}

	log.Info("Creating HTTP router")
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/info", s.jsonResponse(appInfo))
	r.HandleFunc("/api/v1/data/get/{id}", s.jsonResponse(getData)).Methods("GET")

	if s.role == roleMaster {
		r.HandleFunc("/api/v1/data/append", s.jsonResponse(appendData)).Methods("POST")
	} else {
		r.HandleFunc("/api/v1/data/set/{id}", s.jsonResponse(setData)).Methods("POST")
	}

	srv := &http.Server{
		Addr:    s.bind,
		Handler: r,
	}

	go func() {
		log.Infof("server is starting at %s", s.bind)
		err := srv.ListenAndServe()
		if err != nil {
			return
		}
	}()

	return srv, nil
}

func (s *Server) doReplication(idx int, data *IncomingData) error {

	jd, err := json.Marshal(data)
	if err != nil {
		return err
	}

	bodyReader := bytes.NewBuffer(jd)
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/data/set/%d", s.replicateTo, idx), bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.replClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-ok status code from replica: %d", resp.StatusCode)
	}

	return nil
}
