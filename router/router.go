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
	"github.com/viert/bookstore/server"
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

// Router represents router server object
type Router struct {
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

// NewRouter creates and configures a router server with a given config
func NewRouter(cfg *config.RouterCfg) *Router {
	r := &Router{
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
		r.upstreams = append(r.upstreams, ucfg)
	}
	return r
}

func (r *Router) configureUpstreams() (outError error) {
	var si *storageInstance

	for _, ucfg := range r.upstreams {

		masterInfo, err := r.getAppInfo(&ucfg.master)
		if err != nil {
			log.Errorf("error getting info on %s master (%s): %s", ucfg.name, ucfg.master.host, err)
			outError = err
		} else {
			ucfg.instanceID = masterInfo.StorageID
		}

		replInfo, err := r.getAppInfo(&ucfg.replica)
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
		r.writers[ucfg.instanceID] = si
		log.Infof("added writer %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		if _, found := r.readers[ucfg.instanceID]; found {
			outError = fmt.Errorf("StorageID %d has already been used by another instance, skipping", ucfg.instanceID)
			log.Error(outError)
			continue
		}

		r.readers[ucfg.instanceID] = make([]*storageInstance, 0)

		si = &storageInstance{host: ucfg.master.host, isAlive: true}
		r.readers[ucfg.instanceID] = append(r.readers[ucfg.instanceID], si)
		log.Infof("added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		si = &storageInstance{host: ucfg.replica.host, isAlive: true}
		r.readers[ucfg.instanceID] = append(r.readers[ucfg.instanceID], si)
		log.Infof("added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.replica.host, ucfg.instanceID, ucfg.replica.isAlive)
	}
	return
}

func (r *Router) pingUpstreams() {
	for {
		t := time.After(r.checkInt)
		select {
		case <-t:
			for iid, w := range r.writers {
				resp, err := r.getAppInfo(w)
				if err != nil {
					if w.isAlive {
						log.Infof("writer %d (host=%s) becomes dead due to ping error: %s", iid, w.host, err)
						r.writerLock.Lock()
						w.isAlive = false
						r.writerLock.Unlock()
					}
				} else {
					newAlive := !resp.IsFull
					if newAlive {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) becomes alive", iid, w.host)
							r.writerLock.Lock()
							w.isAlive = newAlive
							r.writerLock.Unlock()
						}
					} else {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) is full, thus marked as dead", iid, w.host)
							r.writerLock.Lock()
							w.isAlive = newAlive
							r.writerLock.Unlock()
						}
					}
				}
			}

			for iid, rlist := range r.readers {
				for _, rd := range rlist {
					_, err := r.getAppInfo(rd)
					if err != nil {
						if rd.isAlive {
							log.Infof("reader %d (host=%s) becomes dead due to ping error: %s", iid, rd.host, err)
							r.readerLock.Lock()
							rd.isAlive = false
							r.readerLock.Unlock()
						}
					} else {
						if !rd.isAlive {
							log.Infof("reader %d (host=%s) becomes alive", iid, rd.host)
							r.readerLock.Lock()
							rd.isAlive = true
							r.readerLock.Unlock()
						}
					}
				}
			}

		case <-r.pingerStop:
			break
		}
	}
}

// Start starts the server and background pinger
func (r *Router) Start() error {
	err := r.configureUpstreams()
	if err != nil && r.panic {
		return fmt.Errorf("panic due to upstream failure (and panic_on_faulty flag)")
	}

	if len(r.readers) == 0 || len(r.writers) == 0 {
		err = fmt.Errorf("not enough instances to work with (%d readers and %d writers)", len(r.readers), len(r.writers))
		log.Error(err)
		return err
	}

	mr := mux.NewRouter()
	mr.HandleFunc("/put", r.putData).Methods("POST")
	mr.HandleFunc("/get/{instanceID}/{itemID}", r.getData).Methods("GET")

	r.srv = &http.Server{
		Addr:    r.bind,
		Handler: mr,
	}

	rand.Seed(time.Now().UnixNano())
	go r.pingUpstreams()

	go func() {
		log.Infof("server is starting at %s", r.bind)
		err := r.srv.ListenAndServe()
		if err != nil {
			return
		}
	}()

	return nil

}

// Stop stops the http server and background jobs
func (r *Router) Stop() {
	r.pingerStop <- true
	r.srv.Shutdown(nil)
}

func (r *Router) getAppInfo(si *storageInstance) (*server.InfoResponse, error) {
	cli := &http.Client{
		Timeout: r.storageTimeout,
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

	var info server.InfoResponse
	err = json.Unmarshal(content, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}
