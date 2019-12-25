package httpclient

import (
	"io"
	"net/http"
)

type httpReader struct {
	body io.ReadCloser

	rsp     *http.Response
	readErr error
}

func (r *httpReader) RspParams() ([]byte, bool) {
	p := r.rsp.Header.Get("X-YT-Response-Parameters")
	if p == "" {
		return nil, false
	}
	return []byte(p), true
}

func (r *httpReader) Read(data []byte) (n int, err error) {
	if r.readErr != nil {
		err = r.readErr
		return
	}

	n, err = r.body.Read(data)

	if err == io.EOF {
		ytErr, decodeErr := decodeYTErrorFromHeaders(r.rsp.Trailer)
		if ytErr != nil || err != nil {
			if decodeErr != nil {
				r.readErr = decodeErr
			} else if ytErr != nil {
				r.readErr = ytErr
			}
		}

		return
	}

	return
}

func (r *httpReader) Close() error {
	if r.body != nil {
		return r.body.Close()
	}

	return nil
}
