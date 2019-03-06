package ypath

import (
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yson"
	"github.com/mitchellh/copystructure"
)

// ReadLimit is either table key or row index.
type ReadLimit struct {
	Key      []interface{} `yson:"key,omitempty"`
	RowIndex *int64        `yson:"row_index,omitempty"`
}

// RowIndex creates ReadLimit from row index.
func RowIndex(index int64) ReadLimit {
	return ReadLimit{RowIndex: &index}
}

// RowIndex creates ReadLimit from table key.
func Key(values ...interface{}) ReadLimit {
	if values == nil {
		values = []interface{}{}
	}
	return ReadLimit{Key: values}
}

// Range is subset of table rows defined by lower and upper bound.
type Range struct {
	Lower *ReadLimit `yson:"lower_limit,omitempty"`
	Upper *ReadLimit `yson:"upper_limit,omitempty"`
	Exact *ReadLimit `yson:"exact,omitempty"`
}

// Full is Range corresponding to full table.
func Full() Range {
	return Range{}
}

// StartingFrom is Range of all rows starting from lower read limit and up to table end.
func StartingFrom(lower ReadLimit) Range {
	return Range{Lower: &lower}
}

// UpTo is Range of all rows starting from the first row and up to upper read limit.
func UpTo(lower ReadLimit) Range {
	return Range{Upper: &lower}
}

// Interval is Range of all rows between upper and lower limit.
func Interval(lower ReadLimit, upper ReadLimit) Range {
	return Range{Lower: &lower, Upper: &upper}
}

// Exact is Range of all rows with key matching read limit.
func Exact(r ReadLimit) Range {
	return Range{Exact: &r}
}

// Rich is YPath representation suitable for manipulation by code.
//
// Note that unlike Path, all methods of Rich mutate receiver.
//
//     p := ypath.NewRich("//home/foo/bar").
//         AddRange(ypath.Interval(ypath.RowIndex(0), ypath.RowIndex(1000))).
//         SetColumns([]string{"time", "message"})
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

// NewRich creates new Rich.
//
// This function does not normalize Path and does no validation, use Parse if you need to parse attributes from Path.
func NewRich(path string) *Rich {
	return &Rich{Path: Path(path)}
}

// MarhsalYSON is implementation yson.StreamMarshaler interface.
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

// YPath is implementation of YPath interface.
func (_ Rich) YPath() {}

// Copy returns deep copy of p.
func (p *Rich) Copy() *Rich {
	return copystructure.Must(copystructure.Copy(p)).(*Rich)
}

// SetSchema updates schema attribute of p.
func (p *Rich) SetSchema(schema schema.Schema) *Rich {
	p.Schema = &schema
	return p
}

// SetColumns updates columns attribute of p.
func (p *Rich) SetColumns(columns []string) *Rich {
	p.Columns = columns
	return p
}

// SetSortedBy updates sorted_by attribute of p.
func (p *Rich) SetSortedBy(keysColumns []string) *Rich {
	p.SortedBy = keysColumns
	return p
}

// SetAppend sets append attribute of p.
func (p *Rich) SetAppend() *Rich {
	appendAttr := true
	p.Append = &appendAttr
	return p
}

// AddRange adds element to ranges attribute of to p.
func (p *Rich) AddRange(r Range) *Rich {
	p.Ranges = append(p.Ranges, r)
	return p
}

// SetErasure updates erasure_codec attribute of p.
func (p *Rich) SetErasure(erasure ErasureCodec) *Rich {
	p.Erasure = erasure
	return p
}

// SetCompression updates compression_codec attribute of p.
func (p *Rich) SetCompression(compression CompressionCodec) *Rich {
	p.Compression = compression
	return p
}

// Child append name to Path.
func (p *Rich) Child(name string) *Rich {
	p.Path = p.Path.Child(name)
	return p
}
