package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/viert/bookstore/common"
)

// InfoResponse is a json-marked-up structure for info handler
type InfoResponse struct {
	AppName       string `json:"app_name"`
	StorageID     uint64 `json:"storage_id"`
	ChunkSize     int    `json:"chunk_size"`
	ChunkDataSize int    `json:"chunk_data_size"`
	NumChunks     int    `json:"num_chunks"`
	ServerType    string `json:"server_type"`
	IsFull        bool   `json:"is_full"`
}

// IncomingData is a json-marked-up structure for incoming data
type IncomingData struct {
	Data string `json:"data"`
}

type WriteDataResponse struct {
	ID int `json:"id"`
}

func (s *Server) appInfo(r *http.Request) (interface{}, error) {
	srvType := "replica"
	if s.role == roleMaster {
		srvType = "master"
	}
	return &InfoResponse{
		AppName:       "bookstore",
		StorageID:     s.storage.GetID(),
		ChunkSize:     s.storage.GetChunkSize(),
		ChunkDataSize: s.storage.GetChunkDataSize(),
		NumChunks:     s.storage.GetNumChunks(),
		ServerType:    srvType,
		IsFull:        s.storage.IsFull(),
	}, nil
}

type DataItem struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

type DataListResponse struct {
	Items []*DataItem `json:"items"`
}

func (s *Server) getData(r *http.Request) (interface{}, error) {
	vars := mux.Vars(r)
	tokens := strings.Split(vars["id"], ",")

	dlr := &DataListResponse{Items: make([]*DataItem, 0)}
	for _, token := range tokens {
		id, err := strconv.ParseInt(token, 10, 32)
		if err != nil {
			return nil, common.HTTPError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("invalid id '%s'", token),
			}
		}

		data, err := s.storage.Read(int(id))
		if err != nil {
			return nil, common.HTTPError{
				Message: fmt.Sprintf("error reading item at position %d: %s", id, err),
				Code:    http.StatusInternalServerError,
			}
		}

		item := &DataItem{
			ID:   int(id),
			Data: string(data),
		}
		dlr.Items = append(dlr.Items, item)
	}
	return dlr, nil
}

func getIncomingData(r *http.Request) (*IncomingData, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		return nil, common.HTTPError{
			Message: "this handler accepts JSON data only",
			Code:    http.StatusBadRequest,
		}
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, common.HTTPError{
			Message: fmt.Sprintf("error reading request body: %s", err),
			Code:    http.StatusInternalServerError,
		}
	}

	var input IncomingData
	err = json.Unmarshal(body, &input)
	if err != nil {
		return nil, common.HTTPError{
			Message: fmt.Sprintf("error parsing json data: %s", err),
			Code:    http.StatusBadRequest,
		}
	}

	if input.Data == "" {
		return nil, common.HTTPError{
			Message: "input data is empty",
			Code:    http.StatusBadRequest,
		}
	}

	return &input, nil
}

func (s *Server) appendData(r *http.Request) (interface{}, error) {
	input, err := getIncomingData(r)
	if err != nil {
		return nil, err
	}

	idx, err := s.storage.Write([]byte(input.Data), func(idx int) error {
		if !s.replicate {
			return nil
		}
		return s.doReplication(idx, input)
	})

	if err != nil {
		return nil, common.HTTPError{
			Message: fmt.Sprintf("error writing data to storage: %s", err),
			Code:    http.StatusInternalServerError,
		}
	}

	return &WriteDataResponse{ID: idx}, nil
}

func (s *Server) setData(r *http.Request) (interface{}, error) {
	vars := mux.Vars(r)

	idx, err := strconv.ParseInt(vars["id"], 10, 32)
	if err != nil {
		return nil, common.HTTPError{
			Message: fmt.Sprintf("invalid id '%s'", vars["id"]),
			Code:    http.StatusBadRequest,
		}
	}

	input, err := getIncomingData(r)
	if err != nil {
		return nil, err
	}

	_, err = s.storage.WriteTo([]byte(input.Data), int(idx), func(idx int) error {
		if !s.replicate {
			return nil
		}
		return s.doReplication(idx, input)
	})

	if err != nil {
		return nil, common.HTTPError{
			Message: fmt.Sprintf("error writing data to storage: %s", err),
			Code:    http.StatusInternalServerError,
		}
	}

	return &WriteDataResponse{ID: int(idx)}, nil

}
