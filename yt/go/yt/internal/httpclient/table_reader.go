package httpclient

import (
	"context"
	"io"
	"reflect"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/smartreader"
)

type tableReader struct {
	raw     io.ReadCloser
	y       *yson.Reader
	value   []byte
	err     error
	end     bool
	started bool

	rspParams tableReaderRspParams
}

func (r *tableReader) Scan(value any) error {
	if r.err != nil {
		return r.err
	}
	if !r.started {
		return xerrors.New("call to Scan() before calling Next()")
	}
	if r.end {
		return xerrors.New("call to Scan() after EOF")
	}

	zeroInitialize(value)
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
	return r.err == nil
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

type tableReaderRspParams struct {
	StartRowIndex       *int64 `yson:"start_row_index"`
	ApproximateRowCount *int64 `yson:"approximate_row_count"`
}

func (r *tableReader) setRspParams(params *tableReaderRspParams) error {
	r.rspParams = *params
	return nil
}

func decodeRspParams(ys []byte) (*tableReaderRspParams, error) {
	var params tableReaderRspParams
	if err := yson.Unmarshal(ys, &params); err != nil {
		return nil, err
	}
	return &params, nil
}

func (r *tableReader) StartRowIndex() (int64, bool) {
	if r.rspParams.StartRowIndex != nil {
		return *r.rspParams.StartRowIndex, true
	} else {
		return 0, false
	}
}

func (r *tableReader) ApproximateRowCount() (int64, bool) {
	if r.rspParams.ApproximateRowCount != nil {
		return *r.rspParams.ApproximateRowCount, true
	} else {
		return 0, false
	}
}

var _ yt.TableReader = (*tableReader)(nil)

func newTableReader(r io.ReadCloser) *tableReader {
	return &tableReader{raw: r, y: yson.NewReaderKind(r, yson.StreamListFragment)}
}

func zeroInitialize(v any) {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Ptr {
		return
	}

	value = value.Elem()
	value.Set(reflect.Zero(value.Type()))
}

func (c *httpClient) ReadTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.ReadTableOptions,
) (r yt.TableReader, err error) {
	if options != nil && options.Smart != nil && *options.Smart && !options.Unordered {
		opts := *options
		opts.Smart = nil

		tx, err := c.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		return smartreader.NewReader(ctx, tx, true, c.log, path, &opts)
	} else {
		return c.Encoder.ReadTable(ctx, path, options)
	}
}
