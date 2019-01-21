package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/viert/bookstore/web"
)

type putResponse struct {
	InstanceID uint64 `json:"instance_id"`
	ItemID     int    `json:"item_id"`
}

func (s *Server) proxyData(hosts []string, itemID string) ([]byte, error) {
	cli := &http.Client{Timeout: s.storageTimeout}
	if len(hosts) == 1 {
		host := hosts[0]
		url := fmt.Sprintf("http://%s/api/v1/data/get/%s", host, itemID)
		log.Debugf("getting data from %s", url)
		resp, err := cli.Get(url)
		if err != nil {
			log.Debugf("error getting data from %s: %s", host, err)
			return nil, err
		}
		return ioutil.ReadAll(resp.Body)
	}

	for rt := 3; rt > 0; rt-- {
		idx := rand.Intn(len(hosts))
		host := hosts[idx]
		url := fmt.Sprintf("http://%s/api/v1/data/get/%s", host, itemID)
		log.Debugf("getting data from %s", url)
		resp, err := cli.Get(url)
		if err != nil {
			log.Debugf("error getting data from %s: %s. retries left: %d", host, err, rt-1)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Debugf("non-ok status code from %s: %d retries left: %d", host, resp.StatusCode, rt-1)
			if resp.StatusCode >= 500 {
				continue
			}
			log.Debugf("giving up retries due to non-500 status code")
			break
		}
		return ioutil.ReadAll(resp.Body)
	}
	return nil, fmt.Errorf("no more retries left")
}

func (s *Server) putData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "only application/json body is allowed"}`))
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error": "error reading body: %s"}`, err)))
		return
	}

	// var input web.IncomingData
	// err = json.Unmarshal(data, &input)
	// if err != nil {
	// 	w.WriteHeader(http.StatusBadRequest)
	// 	w.Write([]byte(fmt.Sprintf(`{"error": "error unmarshalling body: %s"}`, err)))
	// 	return
	// }

	type writerDesc struct {
		host      string
		storageID uint64
	}

	s.writerLock.RLock()
	availableWriters := make([]writerDesc, 0)
	for iid, writer := range s.writers {
		if !writer.isAlive {
			continue
		}
		availableWriters = append(availableWriters, writerDesc{host: writer.host, storageID: iid})
	}
	s.writerLock.RUnlock()

	for rt := 3; rt > 0; rt-- {
		idx := rand.Intn(len(availableWriters))
		writer := availableWriters[idx]
		url := fmt.Sprintf("http://%s/api/v1/data/append", writer.host)
		cli := &http.Client{Timeout: s.storageTimeout}
		buf := bytes.NewBuffer(data)
		req, err := http.NewRequest("POST", url, buf)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf(`{"error": "error creating post request: %s"}`, err)))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := cli.Do(req)
		if err != nil {
			log.Errorf("error putting data to %s: %s. retries left %d", url, err, rt-1)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Errorf("non-ok status code from %s while putting data (%d). retries left %d", url, resp.StatusCode, rt-1)
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("error reading response body from %s: %s. retries left %d", url, err, rt-1)
			continue
		}

		var respContent web.WriteDataResponse
		err = json.Unmarshal(data, &respContent)
		if err != nil {
			log.Errorf("error unmarshaling response body from %s: %s. retries left %d", url, err, rt-1)
			continue
		}

		outputContent := putResponse{InstanceID: writer.storageID, ItemID: respContent.ID}
		data, err = json.Marshal(outputContent)
		if err != nil {
			log.Errorf("error marshaling response body: %s. retries left %d", err, rt-1)
			continue
		}
		w.Write(data)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(`{"error": "can't write data after 3 retries"}`))
}

func (s *Server) getData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	instanceID, err := strconv.ParseUint(vars["instanceID"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "invalid instanceID"}`))
		return
	}
	itemID := vars["itemID"]

	s.readerLock.RLock()
	if readers, found := s.readers[instanceID]; found {
		aliveReaders := make([]string, 0, 2)
		for _, reader := range readers {
			if reader.isAlive {
				aliveReaders = append(aliveReaders, reader.host)
			}
		}
		s.readerLock.RUnlock()

		if len(aliveReaders) == 0 {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error": "no alive storages available"}`))
			return
		}

		data, err := s.proxyData(aliveReaders, itemID)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(fmt.Sprintf(`{"error": "error getting data from storage: %s}"`, err)))
			return
		}
		w.Write(data)

	} else {
		s.readerLock.RUnlock()
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "instance not found"}`))
		return
	}

}
