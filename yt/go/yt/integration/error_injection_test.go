package integration

import (
	"fmt"
	"io"
	"net/http"
)

type (
	readTruncatingRoundTripper struct {
		n int
	}

	truncatingReader struct {
		r io.ReadCloser
		n int
	}
)

func (t *readTruncatingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if req.URL.Path == "/api/v4/read_table" {
		rsp.Body = &truncatingReader{rsp.Body, t.n}
	}

	return rsp, nil
}

func (r *truncatingReader) Read(buf []byte) (int, error) {
	if r.n == 0 {
		return 0, fmt.Errorf("injected failure")
	}

	if len(buf) > r.n {
		buf = buf[:r.n]
	}

	n, err := r.r.Read(buf)
	r.n -= n
	return n, err
}

func (r *truncatingReader) Close() error {
	return r.r.Close()
}
