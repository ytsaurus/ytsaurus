package httpclient

import (
	"context"
	"io"
	"reflect"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/smartreader"
)

type rawTableReader struct {
	raw       io.ReadCloser
	rspParams tableReaderRspParams
	err       error
}

func (r *rawTableReader) setRspParams(params *tableReaderRspParams) error {
	r.rspParams = *params
	return nil
}

func (r *rawTableReader) StartRowIndex() (int64, bool) {
	if r.rspParams.StartRowIndex != nil {
		return *r.rspParams.StartRowIndex, true
	} else {
		return 0, false
	}
}

func (r *rawTableReader) ApproximateRowCount() (int64, bool) {
	if r.rspParams.ApproximateRowCount != nil {
		return *r.rspParams.ApproximateRowCount, true
	} else {
		return 0, false
	}
}

func (r *rawTableReader) Close() error {
	if r.raw != nil {
		err := r.raw.Close()
		if r.err != nil {
			r.err = err
		}
		r.raw = nil
	}

	return r.err
}

type ysonTableReader struct {
	rawTableReader
	y       *yson.Reader
	value   []byte
	end     bool
	started bool
}

func (r *ysonTableReader) Scan(value any) error {
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

func (r *ysonTableReader) Next() bool {
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

func (r *ysonTableReader) Err() error {
	return r.err
}

type skiffTableReader struct {
	rawTableReader
	skiffDecoder *skiff.Decoder
	end          bool
}

func newSkiffTableReader(r io.ReadCloser, format skiff.Format, tableSchema *schema.Schema) (*skiffTableReader, error) {
	skiffDecoder, err := skiff.NewDecoder(r, format, skiff.WithDecoderTableSchemas(tableSchema))
	if err != nil {
		return nil, err
	}
	return &skiffTableReader{rawTableReader: rawTableReader{raw: r}, skiffDecoder: skiffDecoder}, nil
}

func (r *skiffTableReader) Scan(value any) error {
	if r.err != nil {
		return r.err
	}
	if r.end {
		return xerrors.New("call to Scan() after EOF")
	}

	zeroInitialize(value)
	r.err = r.skiffDecoder.Scan(value)
	return r.err
}

func (r *skiffTableReader) Next() bool {
	if r.err != nil {
		return false
	} else if r.end {
		return false
	}

	r.end = !r.skiffDecoder.Next()
	r.err = r.skiffDecoder.Err()
	return !r.end
}

func (r *skiffTableReader) Err() error {
	if r.err == nil {
		r.err = r.skiffDecoder.Err()
	}
	return r.err
}

type tableReader interface {
	yt.TableReader
	setRspParams(params *tableReaderRspParams) error
}

func newTableReader(r io.ReadCloser, outputFormat any, tableSchema *schema.Schema) (tr tableReader, err error) {
	if outputFormat != nil {
		if skiffFormat, ok := outputFormat.(skiff.Format); ok {
			return newSkiffTableReader(r, skiffFormat, tableSchema)
		}
		return nil, xerrors.Errorf("unexpected output format: %+v", outputFormat)
	}
	return &ysonTableReader{rawTableReader: rawTableReader{raw: r}, y: yson.NewReaderKind(r, yson.StreamListFragment)}, nil
}

type tableReaderRspParams struct {
	StartRowIndex       *int64 `yson:"start_row_index"`
	ApproximateRowCount *int64 `yson:"approximate_row_count"`
}

func decodeRspParams(ys []byte) (*tableReaderRspParams, error) {
	var params tableReaderRspParams
	if err := yson.Unmarshal(ys, &params); err != nil {
		return nil, err
	}
	return &params, nil
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

		r, err := smartreader.NewReader(ctx, tx, true, c.log, path, &opts)
		if err != nil {
			_ = tx.Abort()
			return nil, err
		}

		return r, err
	} else {
		return c.Encoder.ReadTable(ctx, path, options)
	}
}
