package common

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	defaultErrorHTTPCode = http.StatusBadRequest
)

// HTTPError represents an error-compatible struct to hold http status code
// along with the error message
type HTTPError struct {
	Code    int
	Message string
}

func (he HTTPError) Error() string {
	return he.Message
}

// DataHandler is a common API handler receiving http request
// and returning whatever JSON-able data
type DataHandler func(*http.Request) (interface{}, error)

type httpErrorResponse struct {
	Error string `json:"error"`
}

func NewHTTPError(code int, format string, args ...interface{}) HTTPError {
	return HTTPError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

// WriteJSONError is a helper to return API errors to a user in JSON format
func WriteJSONError(w http.ResponseWriter, e error) error {
	var he HTTPError
	var ok bool
	var err error
	var response httpErrorResponse

	w.Header().Set("Content-Type", "application/json")

	he, ok = e.(HTTPError)
	if !ok {
		he = HTTPError{Code: defaultErrorHTTPCode, Message: e.Error()}
	}

	response = httpErrorResponse{Error: he.Error()}
	data, err := json.Marshal(response)
	if err != nil {
		// this shouldn't ever happen
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte(`{"error": "internal server error"}`))
		return err
	}
	w.WriteHeader(he.Code)
	_, err = w.Write(data)
	return err
}

// JSONResponse converts DataHandler to a http.HandlerFunc
func JSONResponse(handler DataHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		responseData, err := handler(r)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		data, err := json.Marshal(responseData)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "internal server error / marshalling error"}`))
			return
		}
		w.Write(data)
	}
}
