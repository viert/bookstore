package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/viert/bookstore/config"
	"github.com/viert/bookstore/storage"
)

const (
	properStorageID  = 104
	anotherStorageID = 107

	standaloneCfg = `[main]
bind = 127.0.0.1:3999
master = true
[storage]
file = /dev/zero
`
	masterCfg = `[main]
bind = 127.0.0.1:4000
master = true
[storage]
file = /dev/zero
[replica]
host = http://127.0.0.1:4001
timeout = 250`
	replicaCfg = `[main]
bind = 127.0.0.1:4001
master = false
[storage]
file = /dev/zero`
)

func startServer(storageID uint64, configString string) (*http.Server, error) {
	mb := storage.NewMemBackend()
	_, err := storage.CreateStorage(mb, 512, 512, storageID)
	if err != nil {
		return nil, err
	}

	st, err := storage.Open(mb)
	if err != nil {
		return nil, err
	}

	cfgReader := bytes.NewBuffer([]byte(configString))
	cfg, err := config.ReadServerConfig(cfgReader)
	if err != nil {
		return nil, err
	}
	srv, err := NewServer(st, cfg).Start()
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func startMaster(storageID uint64) (*http.Server, error) {
	return startServer(storageID, masterCfg)
}

func startReplica(storageID uint64) (*http.Server, error) {
	return startServer(storageID, replicaCfg)
}

func startStandalone(storageID uint64) (*http.Server, error) {
	return startServer(storageID, standaloneCfg)
}

func makeInputBody(data string) ([]byte, error) {
	input := &incomingData{Data: data}
	return json.Marshal(input)
}

func doPostRequest(data string, port int, path string) error {
	cli := &http.Client{Timeout: 250 * time.Millisecond}
	input, err := makeInputBody(data)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(input)
	url := fmt.Sprintf("http://localhost:%d%s", port, path)
	req, err := http.NewRequest("POST", url, buf)
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return err
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-ok status code from master: %d", resp.StatusCode)
	}
	return nil
}

func doAppendRequest(data string, port int) error {
	return doPostRequest(data, port, "/api/v1/data/append")
}

func doGetData(idx int, port int) (string, error) {
	url := fmt.Sprintf("http://localhost:%d/api/v1/data/get/%d", port, idx)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("non-ok response code from server: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var data DataListResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return "", err
	}

	if len(data.Items) != 1 {
		return "", fmt.Errorf("invalid number of items received: %d", len(data.Items))
	}

	return data.Items[0].Data, nil
}

func TestReplication(t *testing.T) {
	r, err := startReplica(properStorageID)
	if err != nil {
		t.Error(err)
	} else {
		defer r.Shutdown(nil)
	}
	time.Sleep(100 * time.Millisecond)

	m, err := startMaster(properStorageID)
	if err != nil {
		t.Error(err)
	} else {
		defer m.Shutdown(nil)
	}

	err = doAppendRequest("my first data", 4000)
	if err != nil {
		t.Error(err)
	}

	masterData, err := doGetData(0, 4000)
	if err != nil {
		t.Error(err)
	}
	replData, err := doGetData(0, 4001)
	if err != nil {
		t.Error(err)
	}

	if masterData != replData {
		t.Error("master and replicationn data don't match")
	}
}
