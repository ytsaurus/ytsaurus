package yterrors

import (
	"fmt"
)

// HTTPError is usually an error received from YT's HTTP load balancer.
type HTTPError struct {
	StatusCode int
	L7Hostname string
	Err        error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http error (code: %d): %s", e.StatusCode, e.Err.Error())
}

func (e *HTTPError) Unwrap() error {
	return e.Err
}

var _ error = &HTTPError{}
