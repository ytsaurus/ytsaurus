package mapreduce

import (
	"io"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yson"
)

type reader struct {
	in     io.Reader
	reader *yson.Reader

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

	d := yson.Decoder{r.reader}
	r.err = d.Decode(&r.value)
	if r.err != nil {
		return false
	}

	r.hasValue = true
	r.lastTableIndex = r.value.TableIndex
	return true
}

func newReader(r io.Reader) *reader {
	return &reader{
		in:     r,
		reader: yson.NewReaderKind(r, yson.StreamListFragment),
		eof:    false,
	}
}

type writer struct {
	out    io.WriteCloser
	writer *yson.Writer

	err error
}

func (w *writer) Write(value interface{}) error {
	if w.err != nil {
		return w.err
	}

	w.writer.Any(value)
	return w.writer.Err()
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.writer.Finish()
	return w.err
}

type jobContext struct {
	in  *reader
	out []*writer
}

func (c *jobContext) finish() error {
	if c.in.err != nil {
		return xerrors.Errorf("input reader error: %w", c.in.err)
	}

	for _, out := range c.out {
		_ = out.Close()

		if out.err != nil {
			return xerrors.Errorf("output writer error: %w", out.err)
		}
	}

	return nil
}

func (c *jobContext) writers() (out []Writer) {
	for _, w := range c.out {
		out = append(out, w)
	}

	return
}

type JobContext interface {
}
