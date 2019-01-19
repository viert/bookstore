package web

import (
	"encoding/json"
	"net/http"
)

type httpError struct {
	msg  string
	code int
}

type errorResponse struct {
	Error string `json:"error"`
}

type dataHandler func(*http.Request, *Server) (interface{}, error)

func (he *httpError) Error() string {
	return he.msg
}

func (s *Server) jsonResponse(handler dataHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		responseData, err := handler(r, s)
		if err != nil {
			if he, ok := err.(*httpError); ok {
				data, err := json.Marshal(&errorResponse{Error: he.msg})
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error": "marshalling error"}`))
					return
				}
				w.WriteHeader(he.code)
				w.Write(data)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "internal error"}`))
			return
		}
		data, err := json.Marshal(responseData)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "marshalling error"}`))
			return
		}
		w.Write(data)
	}
}
