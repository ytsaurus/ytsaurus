package httpclient

import (
	"io"
	"net/http"
)

type httpReader struct {
	body       io.Reader
	bodyCloser io.Closer

	rsp     *http.Response
	readErr error
}

func (r *httpReader) Read(data []byte) (n int, err error) {
	if r.readErr != nil {
		err = r.readErr
		return
	}

	n, err = r.body.Read(data)
	if err != nil {
		ytErr, decodeErr := decodeYTErrorFromHeaders(r.rsp.Trailer)
		if decodeErr != nil {
			r.readErr = decodeErr
		} else if ytErr != nil {
			r.readErr = ytErr
		}
		if r.readErr != nil {
			err = r.readErr
		}

		return
	}

	return
}

func (r *httpReader) Close() error {
	if r.body != nil {
		return r.bodyCloser.Close()
	}

	return nil
}
