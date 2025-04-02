package internal

import (
	"net/http"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yterrors"
)

func NewHTTPError(statusCode int, headers http.Header, body []byte) error {
	bodyErr := "body is empty"
	if len(body) > 0 {
		bodyErr = string(body)
	}
	return &yterrors.HTTPError{
		StatusCode: statusCode,
		L7Hostname: headers.Get("X-L7-Hostname"),
		Err:        xerrors.New(bodyErr),
	}
}
