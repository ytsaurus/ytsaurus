package rpcclient

import (
	"reflect"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yt"
)

var _ yt.TableReader = (*tableReader)(nil)

type tableReader struct {
	rows  []wire.Row
	d     *wire.WireDecoder
	idx   int
	value wire.Row

	err     error
	end     bool
	started bool
}

func newTableReader(rows []wire.Row, d *rpc_proxy.TRowsetDescriptor) (*tableReader, error) {
	nameTable, err := makeNameTable(d)
	if err != nil {
		return nil, err
	}

	r := &tableReader{
		rows: rows,
		d:    wire.NewDecoder(nameTable),
	}

	return r, nil
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

	if err := r.d.UnmarshalRow(r.value, value); err != nil {
		return xerrors.Errorf("unable to deserialize row: %w", err) // todo set r.err?
	}

	return r.err
}

func (r *tableReader) Next() bool {
	r.started = true

	if r.err != nil {
		return false
	} else if r.end {
		return false
	}

	ok := r.idx < len(r.rows)
	if !ok {
		r.end = true
		return false
	} else if r.err != nil {
		return false
	}

	r.value = r.rows[r.idx]
	r.idx++

	return r.err == nil
}

func (r *tableReader) Err() error {
	return r.err
}

func (r *tableReader) Close() error {
	return r.err
}

func zeroInitialize(v any) {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Ptr {
		return
	}

	value = value.Elem()
	value.Set(reflect.Zero(value.Type()))
}
