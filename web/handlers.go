package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type infoResponse struct {
	AppName string `json:"app_name"`
}

type incomingData struct {
	Data string `json:"data"`
}

type writeDataResponse struct {
	ID int `json:"id"`
}

func appInfo(r *http.Request, s *Server) (interface{}, error) {
	return &infoResponse{AppName: "bookstore"}, nil
}

type DataItem struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

type DataListResponse struct {
	Items []*DataItem `json:"items"`
}

func getData(r *http.Request, s *Server) (interface{}, error) {
	vars := mux.Vars(r)
	tokens := strings.Split(vars["id"], ",")

	dlr := &DataListResponse{Items: make([]*DataItem, 0)}
	for _, token := range tokens {
		id, err := strconv.ParseInt(token, 10, 32)
		if err != nil {
			return nil, &httpError{
				msg:  fmt.Sprintf("invalid id '%s'", token),
				code: http.StatusBadRequest,
			}
		}

		data, err := s.storage.Read(int(id))
		if err != nil {
			return nil, &httpError{
				msg:  fmt.Sprintf("error reading item at position %d: %s", id, err),
				code: http.StatusInternalServerError,
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

func getIncomingData(r *http.Request) (*incomingData, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		return nil, &httpError{
			msg:  "this handler accepts JSON data only",
			code: http.StatusBadRequest,
		}
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, &httpError{
			msg:  fmt.Sprintf("error reading request body: %s", err),
			code: http.StatusInternalServerError,
		}
	}

	var input incomingData
	err = json.Unmarshal(body, &input)
	if err != nil {
		return nil, &httpError{
			msg:  fmt.Sprintf("error parsing json data: %s", err),
			code: http.StatusBadRequest,
		}
	}

	if input.Data == "" {
		return nil, &httpError{
			msg:  "input data is empty",
			code: http.StatusBadRequest,
		}
	}

	return &input, nil
}

func appendData(r *http.Request, s *Server) (interface{}, error) {
	input, err := getIncomingData(r)
	if err != nil {
		return nil, err
	}

	idx, err := s.storage.Write([]byte(input.Data))
	if err != nil {
		return nil, &httpError{
			msg:  fmt.Sprintf("error writing data to storage: %s", err),
			code: http.StatusInternalServerError,
		}
	}

	return &writeDataResponse{ID: idx}, nil
}

func setData(r *http.Request, s *Server) (interface{}, error) {
	vars := mux.Vars(r)

	idx, err := strconv.ParseInt(vars["id"], 10, 32)
	if err != nil {
		return nil, &httpError{
			msg:  fmt.Sprintf("invalid id '%s'", vars["id"]),
			code: http.StatusBadRequest,
		}
	}

	input, err := getIncomingData(r)
	if err != nil {
		return nil, err
	}

	_, err = s.storage.WriteTo([]byte(input.Data), int(idx))
	if err != nil {
		return nil, &httpError{
			msg:  fmt.Sprintf("error writing data to storage: %s", err),
			code: http.StatusInternalServerError,
		}
	}

	return &writeDataResponse{ID: int(idx)}, nil

}
