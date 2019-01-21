package common

import (
	"os"

	logging "github.com/op/go-logging"
)

// ConfigureLogging configures logging to a given filename
// If filename is empty, logging is being redirected to os.Stderr
func ConfigureLogging(filename string) (*os.File, error) {
	var backend *logging.LogBackend
	var lf *os.File
	var err error

	if filename != "" {
		lf, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		backend = logging.NewLogBackend(lf, "", 0)
	} else {
		backend = logging.NewLogBackend(os.Stderr, "", 0)
	}
	format := logging.MustStringFormatter(
		`[%{time:2006-01-02 15:04:05.000}] %{level:7s} %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
	return lf, nil
}
