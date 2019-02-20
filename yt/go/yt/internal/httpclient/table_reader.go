package httpclient

import (
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"golang.org/x/xerrors"
	"io"
)

type tableReader struct {
	raw     io.ReadCloser
	y       *yson.Reader
	value   []byte
	err     error
	end     bool
	started bool
}

func (r *tableReader) Scan(value interface{}) error {
	if r.err != nil {
		return r.err
	}
	if !r.started {
		return xerrors.New("call to Scan() before calling Next()")
	}
	if r.end {
		return xerrors.New("call to Scan() after EOF")
	}

	r.err = yson.Unmarshal(r.value, value)
	return r.err
}

func (r *tableReader) Next() bool {
	r.started = true

	if r.err != nil {
		return false
	} else if r.end {
		return false
	}

	var ok bool
	ok, r.err = r.y.NextListItem()
	if !ok {
		r.end = true
		return false
	} else if r.err != nil {
		return false
	}

	r.value, r.err = r.y.NextRawValue()
	if r.err != nil {
		return false
	}

	return true
}

func (r *tableReader) Err() error {
	return r.err
}

func (r *tableReader) Close() error {
	if r.raw != nil {
		err := r.raw.Close()
		if r.err != nil {
			r.err = err
		}
		r.raw = nil
	}

	return r.err
}

var _ yt.TableReader = (*tableReader)(nil)

func newTableReader(r io.ReadCloser) *tableReader {
	return &tableReader{raw: r, y: yson.NewReaderKind(r, yson.StreamListFragment)}
}
