package web

import (
	"net/http"
)

type infoResponse struct {
	AppName string `json:"app_name"`
}

func infoHandler(r *http.Request) (interface{}, error) {
	return &infoResponse{AppName: "bookstore"}, nil
}

type DataItem struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

type DataListResponse struct {
	Items []DataItem `json:"items"`
}
