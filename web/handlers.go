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
