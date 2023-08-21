package httpclient

import "io"

type httpWriter struct {
	errChan <-chan error
	err     error
	w       io.Writer
	c       io.Closer
}

func (w *httpWriter) Write(buf []byte) (n int, err error) {
	n, err = w.w.Write(buf)
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
		if w.c != nil {
			w.err = w.c.Close()
		}
	}

	if w.err == nil {
		w.err = <-w.errChan
	}

	return w.err
}
