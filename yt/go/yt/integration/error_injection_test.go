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

	firstReadTruncatingRoundTripper struct {
		n          int
		readParams []string
	}

	truncatingReader struct {
		r   io.ReadCloser
		n   int
		err error
	}
)

func (t *readTruncatingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if req.URL.Path == "/api/v4/read_table" {
		rsp.Body = &truncatingReader{r: rsp.Body, n: t.n, err: fmt.Errorf("injected failure")}
	}

	return rsp, nil
}

func (t *firstReadTruncatingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if req.URL.Path == "/api/v4/read_table" {
		t.readParams = append(t.readParams, req.Header.Get("X-YT-Parameters"))
		if len(t.readParams) == 1 {
			rsp.Body = &truncatingReader{r: rsp.Body, n: t.n, err: io.ErrUnexpectedEOF}
		}
	}

	return rsp, nil
}

func (r *truncatingReader) Read(buf []byte) (int, error) {
	if r.n == 0 {
		return 0, r.err
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
