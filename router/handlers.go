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
	"github.com/viert/bookstore/server"
)

type putResponse struct {
	InstanceID uint64 `json:"instance_id"`
	ItemID     int    `json:"item_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (rt *Router) proxyData(hosts []string, itemID string) ([]byte, error) {
	cli := &http.Client{Timeout: rt.storageTimeout}
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
				return nil, fmt.Errorf(errData.Error)
			}
			continue
		}
		return ioutil.ReadAll(resp.Body)
	}
	return nil, fmt.Errorf("no more retries left")
}

func (rt *Router) putData(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "only application/json body is allowed"}`))
		return
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error": "error reading body: %s"}`, err)))
		return
	}

	type writerDesc struct {
		host      string
		storageID uint64
	}

	rt.writerLock.RLock()
	availableWriters := make([]writerDesc, 0)
	for iid, writer := range rt.writers {
		if !writer.isAlive {
			continue
		}
		availableWriters = append(availableWriters, writerDesc{host: writer.host, storageID: iid})
	}
	rt.writerLock.RUnlock()

	for retries := 3; retries > 0; retries-- {
		idx := rand.Intn(len(availableWriters))
		writer := availableWriters[idx]
		url := fmt.Sprintf("http://%s/api/v1/data/append", writer.host)
		cli := &http.Client{Timeout: rt.storageTimeout}
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
			log.Errorf("error putting data to %s: %s. retries left %d", url, err, retries-1)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Errorf("non-ok status code from %s while putting data (%d). retries left %d", url, resp.StatusCode, retries-1)
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("error reading response body from %s: %s. retries left %d", url, err, retries-1)
			continue
		}

		var respContent server.WriteDataResponse
		err = json.Unmarshal(data, &respContent)
		if err != nil {
			log.Errorf("error unmarshaling response body from %s: %s. retries left %d", url, err, retries-1)
			continue
		}

		outputContent := putResponse{InstanceID: writer.storageID, ItemID: respContent.ID}
		data, err = json.Marshal(outputContent)
		if err != nil {
			log.Errorf("error marshaling response body: %s. retries left %d", err, retries-1)
			continue
		}
		w.Write(data)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(`{"error": "can't write data after 3 retries"}`))
}

func (rt *Router) getData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	instanceID, err := strconv.ParseUint(vars["instanceID"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "invalid instanceID"}`))
		return
	}
	itemID := vars["itemID"]

	rt.readerLock.RLock()
	if readers, found := rt.readers[instanceID]; found {
		aliveReaders := make([]string, 0, 2)
		for _, reader := range readers {
			if reader.isAlive {
				aliveReaders = append(aliveReaders, reader.host)
			}
		}
		rt.readerLock.RUnlock()

		if len(aliveReaders) == 0 {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error": "no alive storages available"}`))
			return
		}

		data, err := rt.proxyData(aliveReaders, itemID)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(fmt.Sprintf(`{"error": "error getting data from storage: %s"}`, err)))
			return
		}
		w.Write(data)

	} else {
		rt.readerLock.RUnlock()
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "instance not found"}`))
		return
	}

}
