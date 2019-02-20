package httpclient

import (
	"io"
)

type httpWriter struct {
	errChan <-chan error
	err     error
	p       *io.PipeWriter
}

func (w *httpWriter) Write(buf []byte) (n int, err error) {
	n, err = w.p.Write(buf)
	return
}

func (w *httpWriter) Close() error {
	if w.err != nil {
		return w.err
	}

	select {
	case w.err = <-w.errChan:
	default:
	}

	if w.err == nil {
		w.err = w.p.Close()
	}

	if w.err == nil {
		w.err = <-w.errChan
	}

	return w.err
}
