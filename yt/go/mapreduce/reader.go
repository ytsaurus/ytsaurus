package mapreduce

import (
	"io"

	"a.yandex-team.ru/yt/go/yson"
)

// Reader represents input of the job.
//
//     var row MyRow
//     for r.Next() {
//         r.MustScan(&row)
//     }
//
type Reader interface {
	TableIndex() int

	// Scan decodes current value from the input stream.
	//
	// Note, that Scan does not advance input cursor forward.
	//
	// Returns error, only if current row can't be decoded into value.
	// All other errors will terminate current process immediately.
	Scan(value interface{}) error

	// MustScan works like Scan, but terminates current process in case of an error.
	MustScan(value interface{})

	// Advances input stream to the next record.
	Next() bool
}

type reader struct {
	in     io.Reader
	reader *yson.Reader
	ctx    *jobContext

	err            error
	eof            bool
	hasValue       bool
	lastTableIndex int
	value          valueWithControlAttrs
}

type valueWithControlAttrs struct {
	TableIndex int           `yson:"table_index,attr"`
	Value      yson.RawValue `yson:",value"`
}

func (r *reader) TableIndex() int {
	if !r.hasValue {
		panic("TableIndex() called out of sequence")
	}

	return r.lastTableIndex
}

func (r *reader) Scan(value interface{}) error {
	if !r.hasValue {
		panic("Scan() called out of sequence")
	}

	return yson.Unmarshal([]byte(r.value.Value), value)
}

func (r *reader) MustScan(value interface{}) {
	err := r.Scan(value)
	if err != nil {
		r.ctx.onError(err)
	}
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	r.hasValue = false

	if r.eof || r.err != nil {
		return false
	}

	var ok bool
	ok, r.err = r.reader.NextListItem()
	r.eof = !ok
	if r.eof || r.err != nil {
		return false
	}

	d := yson.Decoder{R: r.reader}
	r.err = d.Decode(&r.value)
	if r.err != nil {
		return false
	}

	r.hasValue = true
	r.lastTableIndex = r.value.TableIndex
	return true
}

func newReader(r io.Reader, ctx *jobContext) *reader {
	return &reader{
		in:     r,
		ctx:    ctx,
		reader: yson.NewReaderKind(r, yson.StreamListFragment),
		eof:    false,
	}
}
