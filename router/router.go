package router

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/viert/bookstore/common"

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

func (rt *Router) configureUpstreams() (outError error) {
	var si *storageInstance

	for _, ucfg := range rt.upstreams {

		masterInfo, err := rt.getAppInfo(&ucfg.master)
		if err != nil {
			log.Errorf("error getting info on %s master (%s): %s", ucfg.name, ucfg.master.host, err)
			outError = err
		} else {
			ucfg.instanceID = masterInfo.StorageID
		}

		replInfo, err := rt.getAppInfo(&ucfg.replica)
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
		rt.writers[ucfg.instanceID] = si
		log.Infof("added writer %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		if _, found := rt.readers[ucfg.instanceID]; found {
			outError = fmt.Errorf("StorageID %d has already been used by another instance, skipping", ucfg.instanceID)
			log.Error(outError)
			continue
		}

		rt.readers[ucfg.instanceID] = make([]*storageInstance, 0)

		si = &storageInstance{host: ucfg.master.host, isAlive: true}
		rt.readers[ucfg.instanceID] = append(rt.readers[ucfg.instanceID], si)
		log.Infof("added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.master.host, ucfg.instanceID, ucfg.master.isAlive)

		si = &storageInstance{host: ucfg.replica.host, isAlive: true}
		rt.readers[ucfg.instanceID] = append(rt.readers[ucfg.instanceID], si)
		log.Infof("added reader %s: host=%s storageID=%d isAlive=%v", ucfg.name, ucfg.replica.host, ucfg.instanceID, ucfg.replica.isAlive)
	}
	return
}

func (rt *Router) pingUpstreams() {
	for {
		t := time.After(rt.checkInt)
		select {
		case <-t:
			for iid, w := range rt.writers {
				resp, err := rt.getAppInfo(w)
				if err != nil {
					if w.isAlive {
						log.Infof("writer %d (host=%s) becomes dead due to ping error: %s", iid, w.host, err)
						rt.writerLock.Lock()
						w.isAlive = false
						rt.writerLock.Unlock()
					}
				} else {
					newAlive := !resp.IsFull
					if newAlive {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) becomes alive", iid, w.host)
							rt.writerLock.Lock()
							w.isAlive = newAlive
							rt.writerLock.Unlock()
						}
					} else {
						if newAlive != w.isAlive {
							log.Infof("writer %d (host=%s) is full, thus marked as dead", iid, w.host)
							rt.writerLock.Lock()
							w.isAlive = newAlive
							rt.writerLock.Unlock()
						}
					}
				}
			}

			for iid, rlist := range rt.readers {
				for _, rd := range rlist {
					_, err := rt.getAppInfo(rd)
					if err != nil {
						if rd.isAlive {
							log.Infof("reader %d (host=%s) becomes dead due to ping error: %s", iid, rd.host, err)
							rt.readerLock.Lock()
							rd.isAlive = false
							rt.readerLock.Unlock()
						}
					} else {
						if !rd.isAlive {
							log.Infof("reader %d (host=%s) becomes alive", iid, rd.host)
							rt.readerLock.Lock()
							rd.isAlive = true
							rt.readerLock.Unlock()
						}
					}
				}
			}

		case <-rt.pingerStop:
			break
		}
	}
}

// Start starts the server and background pinger
func (rt *Router) Start() error {
	var err error

	err = rt.configureUpstreams()
	if err != nil && rt.panic {
		return fmt.Errorf("panic due to upstream failure (and panic_on_faulty flag)")
	}

	if len(rt.readers) == 0 || len(rt.writers) == 0 {
		err = fmt.Errorf("not enough instances to work with (%d readers and %d writers)", len(rt.readers), len(rt.writers))
		log.Error(err)
		return err
	}

	r := mux.NewRouter()
	r.HandleFunc("/put", common.JSONResponse(rt.putData)).Methods("POST")
	r.HandleFunc("/get/{instanceID}/{itemID}", common.JSONResponse(rt.getData)).Methods("GET")

	rt.srv = &http.Server{
		Addr:    rt.bind,
		Handler: r,
	}

	rand.Seed(time.Now().UnixNano())
	go rt.pingUpstreams()

	go func() {
		log.Infof("server is starting at %s", rt.bind)
		err = rt.srv.ListenAndServe()
		if err != nil {
			return
		}
	}()

	return nil

}

// Stop stops the http server and background jobs
func (rt *Router) Stop() {
	rt.pingerStop <- true
	rt.srv.Shutdown(nil)
}

func (rt *Router) getAppInfo(si *storageInstance) (*server.InfoResponse, error) {
	cli := &http.Client{
		Timeout: rt.storageTimeout,
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

func (rt *Router) proxyData(hosts []string, itemID string) (*server.DataListResponse, error) {
	var responseBody []byte
	var listResponse server.DataListResponse
	var err error

	cli := &http.Client{Timeout: rt.storageTimeout}
	if len(hosts) == 1 {
		host := hosts[0]
		url := fmt.Sprintf("http://%s/api/v1/data/get/%s", host, itemID)
		log.Debugf("getting data from %s", url)
		resp, err := cli.Get(url)
		if err != nil {
			log.Debugf("error getting data from %s: %s", host, err)
			return nil, common.NewHTTPError(502, "error getting data: %s", err)
		}
		responseBody, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Debugf("error reading response body: %s", err)
			return nil, common.NewHTTPError(500, "error reading response body: %s", err)
		}
	} else {
		for retries := 3; retries > 0; retries-- {
			idx := rand.Intn(len(hosts))
			host := hosts[idx]
			url := fmt.Sprintf("http://%s/api/v1/data/get/%s", host, itemID)
			log.Debugf("getting data from %s", url)
			resp, err := cli.Get(url)
			if err != nil {
				log.Debugf("error getting data from %s: %s. retries left: %d", host, err, retries-1)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				content, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Debugf("status code %d from %s, body can't be read due to an error: %s. retries left: %d", resp.StatusCode, host, err, retries-1)
					continue
				}

				var errData errorResponse
				err = json.Unmarshal(content, &errData)
				if err != nil {
					log.Debugf("status code %d from %s, body can't be unmarshalled due to an error: %s. retries left: %d", resp.StatusCode, host, err, retries-1)
					continue
				}

				log.Debugf("error getting data: %s", errData.Error)
				if resp.StatusCode == http.StatusNotFound {
					// no need to retry if there's no such item
					log.Debugf("status code 404 from %s, giving up", host)
					return nil, common.NewHTTPError(404, errData.Error)
				}
				continue
			}

			responseBody, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Debugf("error reading response body: %s", err)
				return nil, common.NewHTTPError(500, "error reading response body: %s", err)
			}
			break
		}

		if responseBody == nil {
			return nil, common.NewHTTPError(502, "can't get data: no more retries left")
		}
	}

	err = json.Unmarshal(responseBody, &listResponse)
	if err != nil {
		return nil, common.NewHTTPError(500, "error unmarshalling data: %s", err)
	}

	return &listResponse, nil
}
