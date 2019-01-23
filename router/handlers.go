package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/viert/bookstore/common"

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

func (rt *Router) putData(r *http.Request) (interface{}, error) {
	// Checking incoming data
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		return nil, common.NewHTTPError(400, "only application/json body is allowed")
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, common.NewHTTPError(400, "error reading body: %s", err)
	}

	// Getting available writers
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
			return nil, common.NewHTTPError(500, "error creating post request: %s", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := cli.Do(req)
		if err != nil {
			log.Errorf("error putting data to %s: %s. retries left %d", url, err, retries-1)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Errorf("status code %d from %s while putting data (%d). retries left %d", resp.StatusCode, url, resp.StatusCode, retries-1)
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
		return outputContent, nil
	}

	return nil, common.NewHTTPError(500, "can't write data after 3 retries")
}

func (rt *Router) getData(r *http.Request) (interface{}, error) {
	vars := mux.Vars(r)
	instanceID, err := strconv.ParseUint(vars["instanceID"], 10, 64)
	if err != nil {
		return nil, common.NewHTTPError(400, "invalid instance id")
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
			return nil, common.NewHTTPError(502, "no alive storages available")
		}

		data, err := rt.proxyData(aliveReaders, itemID)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else {
		rt.readerLock.RUnlock()
		return nil, common.NewHTTPError(404, "instance not found")
	}

}
