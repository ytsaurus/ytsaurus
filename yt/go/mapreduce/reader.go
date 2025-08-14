package mapreduce

import (
	"io"
	"reflect"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yson"
)

// Reader represents input of the job.
//
//	var row MyRow
//	for r.Next() {
//	    r.MustScan(&row)
//	}
type Reader interface {
	// TableIndex returns table index of the current row.
	TableIndex() int

	// KeySwitch returns true, if current row is the first row with the current key.
	KeySwitch() bool

	// RowIndex returns row index of the current row.
	RowIndex() int64

	// RangeIndex returns range index of the current row.
	RangeIndex() int

	// Scan decodes current value from the input stream.
	//
	// Note, that Scan does not advance input cursor forward.
	//
	// Returns error, only if current row can't be decoded into value.
	// All other errors will terminate current process immediately.
	Scan(value any) error

	// MustScan works like Scan, but terminates current process in case of an error.
	MustScan(value any)

	// Advances input stream to the next record.
	Next() bool

	// Err returns the error that occurred while trying to advance input stream to the next record.
	Err() error
}

type ysonReader struct {
	in     io.Reader
	reader *yson.Reader
	ctx    *jobContext

	err      error
	eof      bool
	hasValue bool

	lastTableIndex int
	lastKeySwitch  bool
	lastRowIndex   int64
	lastRangeIndex int

	value valueWithControlAttrs
}

type valueWithControlAttrs struct {
	TableIndex *int   `yson:"table_index,attr"`
	KeySwitch  *bool  `yson:"key_switch,attr"`
	RowIndex   *int64 `yson:"row_index,attr"`
	RangeIndex *int   `yson:"range_index,attr"`

	Value yson.RawValue `yson:",value"`
}

func (r *ysonReader) TableIndex() int {
	if !r.hasValue {
		panic("TableIndex() called out of sequence")
	}

	return r.lastTableIndex
}

func (r *ysonReader) KeySwitch() bool {
	if !r.hasValue {
		panic("KeySwitch() called out of sequence")
	}

	return r.lastKeySwitch
}

func (r *ysonReader) RowIndex() int64 {
	if !r.hasValue {
		panic("RowIndex() called out of sequence")
	}

	return r.lastRowIndex
}

func (r *ysonReader) RangeIndex() int {
	if !r.hasValue {
		panic("RangeIndex() called out of sequence")
	}

	return r.lastRangeIndex
}

func (r *ysonReader) Scan(value any) error {
	if !r.hasValue {
		panic("Scan() called out of sequence")
	}

	return yson.Unmarshal(r.value.Value, value)
}

func (r *ysonReader) MustScan(value any) {
	err := r.Scan(value)
	if err != nil {
		r.ctx.onError(err)
	}
}

func (r *ysonReader) Err() error {
	return r.err
}

func (r *ysonReader) Next() bool {
	r.hasValue = false

	if r.eof || r.err != nil {
		return false
	}

	r.lastKeySwitch = false
	var rowIndexUpdated bool

	for {
		var ok bool
		ok, r.err = r.reader.NextListItem()
		r.eof = !ok
		if r.eof || r.err != nil {
			return false
		}

		d := yson.Decoder{R: r.reader}
		zeroInitialize(&r.value)
		r.err = d.Decode(&r.value)
		if r.err != nil {
			return false
		}

		if r.value.KeySwitch != nil {
			r.lastKeySwitch = true
			continue
		}

		if r.value.TableIndex != nil {
			r.lastTableIndex = *r.value.TableIndex
			continue
		}

		if r.value.RowIndex != nil {
			r.lastRowIndex = *r.value.RowIndex
			rowIndexUpdated = true
			continue
		}

		if r.value.RangeIndex != nil {
			r.lastRangeIndex = *r.value.RangeIndex
			continue
		}

		r.hasValue = true
		if !rowIndexUpdated {
			r.lastRowIndex++
		}
		return true
	}
}

func newYSONReader(r io.Reader, ctx *jobContext) *ysonReader {
	return &ysonReader{
		in:     r,
		ctx:    ctx,
		reader: yson.NewReaderKind(r, yson.StreamListFragment),
		eof:    false,
	}
}

type skiffReader struct {
	*skiff.Decoder
	ctx *jobContext
}

func newSkiffReader(
	r io.Reader,
	ctx *jobContext,
	inputSkiffFormat *skiff.Format,
	tableSchemas []*schema.Schema,
) (*skiffReader, error) {
	in, err := skiff.NewDecoder(r, *inputSkiffFormat, skiff.WithDecoderTableSchemas(tableSchemas...))
	if err != nil {
		return nil, xerrors.Errorf("failed to create skiff decoder: %w", err)
	}
	return &skiffReader{Decoder: in, ctx: ctx}, nil
}

func (r *skiffReader) MustScan(value any) {
	if err := r.Scan(value); err != nil {
		r.ctx.onError(err)
	}
}

func zeroInitialize(v any) {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Ptr {
		return
	}

	value = value.Elem()
	value.Set(reflect.Zero(value.Type()))
}
