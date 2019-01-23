package router

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/op/go-logging"
	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/web"
)

type storageInstance struct {
	isAlive bool
	host    string
}

type upstreamConfig struct {
	instanceID uint64
	name       string
	master     storageInstance
	replica    storageInstance
}

// Server represents router server object
type Server struct {
	bind           string
	upstreams      []*upstreamConfig
	readers        map[uint64][]*storageInstance
	writers        map[uint64]*storageInstance
	panic          bool
	checkInt       time.Duration
	storageTimeout time.Duration
	srv            *http.Server
	pingerStop     chan bool
	readerLock     sync.RWMutex
	writerLock     sync.RWMutex
}

var (
	log = logging.MustGetLogger("router")
)

// NewServer creates and configures a router server with a given config
func NewServer(cfg *config.RouterCfg) *Server {
	s := &Server{
		bind:           cfg.Bind,
		upstreams:      make([]*upstreamConfig, 0),
		panic:          cfg.PanicOnFaultyInstances,
		readers:        make(map[uint64][]*storageInstance),
		writers:        make(map[uint64]*storageInstance),
		storageTimeout: cfg.StorageTimeout,
		checkInt:       cfg.StorageCheckInterval,
		pingerStop:     make(chan bool),
	}

	for name, hp := range cfg.Upstreams {
		ucfg := &upstreamConfig{
			name:       name,
			master:     storageInstance{host: hp.Master, isAlive: false},
			replica:    storageInstance{host: hp.Replica, isAlive: false},
			instanceID: 0,
		}
		s.upstreams = append(s.upstreams, ucfg)
	}
	return s
}

func (s *Server) configureUpstreams() (outError error) {
	var si *storageInstance

	for _, ucfg := range s.upstreams {

		masterInfo, err := s.getAppInfo(&ucfg.master)
		if err != nil {
			log.Errorf("error getting info on %s master (%s): %s", ucfg.name, ucfg.master.host, err)
			outError = err
		} else {
			ucfg.instanceID = masterInfo.StorageID
		}

		replInfo, err := s.getAppInfo(&ucfg.replica)
		if err != nil {
			log.Errorf("error getting info on %s replica (%s): %s", ucfg.name, ucfg.master.host, err)
			outError = err
		} else {
			ucfg.instanceID = replInfo.StorageID
		}

		if outError == nil {
			if masterInfo.StorageID != replInfo.StorageID {
				outError = fmt.Errorf("%s instances' storage ids don't match", ucfg.name)
				log.Error(outError)
				continue
			}
			ucfg.master.isAlive = true
			ucfg.replica.isAlive = true
		} else {
			if ucfg.instanceID == 0 {
				outError = fmt.Errorf("%s instances are not accessible so can't be used", ucfg.name)
				log.Error(outError)
				continue
			}
			log.Warningf("%s instances are added unchecked due to errors during getting info", ucfg.name)
		}

		si = &storageInstance{host: ucfg.master.host, isAlive: true}
		s.writers[ucfg.instanceID] = si
		log.Infof("Added writer %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		if _, found := s.readers[ucfg.instanceID]; found {
			outError = fmt.Errorf("StorageID %d has already been used by another instance, skipping", ucfg.instanceID)
			log.Error(outError)
			continue
		}

		// !! this is another copy of ucfg.master
		s.readers[ucfg.instanceID] = make([]*storageInstance, 0)

		si = &storageInstance{host: ucfg.master.host, isAlive: true}
		s.readers[ucfg.instanceID] = append(s.readers[ucfg.instanceID], si)
		log.Infof("Added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		si = &storageInstance{host: ucfg.replica.host, isAlive: true}
		s.readers[ucfg.instanceID] = append(s.readers[ucfg.instanceID], si)
		log.Infof("Added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.replica.host, ucfg.instanceID, ucfg.replica.isAlive)
	}
	return
}

func (s *Server) pingUpstreams() {
	for {
		t := time.After(s.checkInt)
		select {
		case <-t:
			for iid, w := range s.writers {
				resp, err := s.getAppInfo(w)
				if err != nil {
					if w.isAlive {
						log.Infof("writer %d (host=%s) becomes dead due to ping error: %s", iid, w.host, err)
						s.writerLock.Lock()
						w.isAlive = false
						s.writerLock.Unlock()
					}
				} else {
					newAlive := !resp.IsFull
					if newAlive {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) becomes alive", iid, w.host)
							s.writerLock.Lock()
							w.isAlive = newAlive
							s.writerLock.Unlock()
						}
					} else {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) is full, thus marked as dead", iid, w.host)
							s.writerLock.Lock()
							w.isAlive = newAlive
							s.writerLock.Unlock()
						}
					}
				}
			}

			for iid, rlist := range s.readers {
				for _, r := range rlist {
					_, err := s.getAppInfo(r)
					if err != nil {
						if r.isAlive {
							log.Infof("reader %d (host=%s) becomes dead due to ping error: %s", iid, r.host, err)
							s.readerLock.Lock()
							r.isAlive = false
							s.readerLock.Unlock()
						}
					} else {
						if !r.isAlive {
							log.Infof("reader %d (host=%s) becomes alive", iid, r.host)
							s.readerLock.Lock()
							r.isAlive = true
							s.readerLock.Unlock()
						}
					}
				}
			}

		case <-s.pingerStop:
			break
		}
	}
}

// Start starts the server and background pinger
func (s *Server) Start() error {
	err := s.configureUpstreams()
	if err != nil && s.panic {
		return fmt.Errorf("panic due to upstream failure (and panic_on_faulty flag)")
	}

	if len(s.readers) == 0 || len(s.writers) == 0 {
		err = fmt.Errorf("not enough instances to work with (%d readers and %d writers)", len(s.readers), len(s.writers))
		log.Error(err)
		return err
	}

	r := mux.NewRouter()
	r.HandleFunc("/put", s.putData).Methods("POST")
	r.HandleFunc("/get/{instanceID}/{itemID}", s.getData).Methods("GET")

	s.srv = &http.Server{
		Addr:    s.bind,
		Handler: r,
	}

	rand.Seed(time.Now().UnixNano())
	go s.pingUpstreams()

	go func() {
		log.Infof("server is starting at %s", s.bind)
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

func (s *Server) getAppInfo(si *storageInstance) (*web.InfoResponse, error) {
	cli := &http.Client{
		Timeout: s.storageTimeout,
	}

	url := fmt.Sprintf("http://%s/api/v1/info", si.host)
	resp, err := cli.Get(url)
	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var info web.InfoResponse
	err = json.Unmarshal(content, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}
