package ypath

import (
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yson"
)

type ReadLimit struct {
	Key      []interface{} `yson:"key,omitempty"`
	RowIndex *int64        `yson:"row_index,omitempty"`
}

func RowIndex(index int64) ReadLimit {
	return ReadLimit{RowIndex: &index}
}

func Key(values ...interface{}) ReadLimit {
	return ReadLimit{Key: values}
}

type Range struct {
	Lower *ReadLimit `yson:"lower_limit,omitempty"`
	Upper *ReadLimit `yson:"lower_limit,omitempty"`
	Exact *ReadLimit `yson:"lower_limit,omitempty"`
}

func Interval(lower ReadLimit, upper ReadLimit) Range {
	return Range{Lower: &lower, Upper: &upper}
}

func Exact(r ReadLimit) Range {
	return Range{Exact: &r}
}

type Rich struct {
	Path Path `yson:",value"`

	Append   *bool          `yson:"append,attr,omitempty"`
	SortedBy []string       `yson:"sorted_by,attr,omitempty"`
	Ranges   []Range        `yson:"ranges,attr,omitempty"`
	Columns  []string       `yson:"columns,attr,omitempty"`
	Schema   *schema.Schema `yson:"schema,attr,omitempty"`

	Compression CompressionCodec `yson:"compression_codec,attr,omitempty"`
	Erasure     ErasureCodec     `yson:"erasure_codec,attr,omitempty"`

	TransactionID interface{}       `yson:"transaction_id,attr,omitempty"`
	RenameColumns map[string]string `yson:"rename_columns,attr,omitempty"`
}

func (r Rich) MarshalYSON(w *yson.Writer) error {
	type ClearPath Rich
	clear := ClearPath(r)
	ys, err := yson.Marshal(&clear)
	if err != nil {
		return err
	}

	w.RawNode(ys)
	return w.Err()
}

func (_ Rich) YPath() {}

func Parse(path string) (p Rich, err error) {
	var attrs, rest []byte
	attrs, rest, err = yson.SliceYPath([]byte(path))
	if err != nil {
		return
	}

	_, _ = attrs, rest
	panic("not implemented")
	return
}

func (p *Rich) SetSchema(schema schema.Schema) *Rich {
	p.Schema = &schema
	return p
}

func (p *Rich) SetColumns(columns []string) *Rich {
	p.Columns = columns
	return p
}

func (p *Rich) SetSortedBy(keysColumns []string) *Rich {
	p.SortedBy = keysColumns
	return p
}

func (p *Rich) SetAppend() *Rich {
	appendAttr := true
	p.Append = &appendAttr
	return p
}

func (p *Rich) AddRange(r Range) *Rich {
	p.Ranges = append(p.Ranges, r)
	return p
}

func (p *Rich) SetErasure(erasure ErasureCodec) *Rich {
	p.Erasure = erasure
	return p
}

func (p *Rich) SetCompression(compression CompressionCodec) *Rich {
	p.Compression = compression
	return p
}

func (p *Rich) Child(name string) *Rich {
	p.Path = p.Path.Child(name)
	return p
}
