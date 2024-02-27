package ypath

import (
	"github.com/mitchellh/copystructure"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

// ReadLimit is either table key or row index.
type ReadLimit struct {
	Key      []any  `yson:"key,omitempty"`
	RowIndex *int64 `yson:"row_index,omitempty"`
}

// RowIndex creates ReadLimit from row index.
func RowIndex(index int64) ReadLimit {
	return ReadLimit{RowIndex: &index}
}

// RowIndex creates ReadLimit from table key.
func Key(values ...any) ReadLimit {
	if values == nil {
		values = []any{}
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
//	p := ypath.NewRich("//home/foo/bar").
//	    AddRange(ypath.Interval(ypath.RowIndex(0), ypath.RowIndex(1000))).
//	    SetColumns([]string{"time", "message"})
type Rich struct {
	Path Path `yson:",value"`

	Append   *bool          `yson:"append,attr,omitempty"`
	SortedBy []string       `yson:"sorted_by,attr,omitempty"`
	Ranges   []Range        `yson:"ranges,attr,omitempty"`
	Columns  []string       `yson:"columns,attr,omitempty"`
	Schema   *schema.Schema `yson:"schema,attr,omitempty"`
	FileName string         `yson:"file_name,attr,omitempty"`

	Teleport bool `yson:"teleport,attr,omitempty"`
	Foreign  bool `yson:"foreign,attr,omitempty"`

	Compression CompressionCodec `yson:"compression_codec,attr,omitempty"`
	Erasure     ErasureCodec     `yson:"erasure_codec,attr,omitempty"`

	TransactionID any               `yson:"transaction_id,attr,omitempty"`
	RenameColumns map[string]string `yson:"rename_columns,attr,omitempty"`
}

// NewRich creates new Rich.
//
// This function does not normalize Path and does no validation, use Parse if you need to parse attributes from Path.
func NewRich(path string) *Rich {
	return &Rich{Path: Path(path)}
}

// MarshalYSON is implementation yson.StreamMarshaler interface.
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
func (r Rich) YPath() Path {
	return r.Path.YPath()
}

// Copy returns deep copy of p.
func (r *Rich) Copy() *Rich {
	return copystructure.Must(copystructure.Copy(r)).(*Rich)
}

// SetSchema updates schema attribute of p.
func (r *Rich) SetSchema(schema schema.Schema) *Rich {
	r.Schema = &schema
	return r
}

// SetColumns updates columns attribute of p.
func (r *Rich) SetColumns(columns []string) *Rich {
	r.Columns = columns
	return r
}

// SetSortedBy updates sorted_by attribute of p.
func (r *Rich) SetSortedBy(keysColumns []string) *Rich {
	r.SortedBy = keysColumns
	return r
}

// SetAppend sets append attribute of p.
func (r *Rich) SetAppend() *Rich {
	r.Append = ptr.Bool(true)
	return r
}

// AddRange adds element to ranges attribute of to p.
func (r *Rich) AddRange(rr Range) *Rich {
	r.Ranges = append(r.Ranges, rr)
	return r
}

// SetErasure updates erasure_codec attribute of p.
func (r *Rich) SetErasure(erasure ErasureCodec) *Rich {
	r.Erasure = erasure
	return r
}

// SetCompression updates compression_codec attribute of p.
func (r *Rich) SetCompression(compression CompressionCodec) *Rich {
	r.Compression = compression
	return r
}

// Child append name to Path.
func (r *Rich) Child(name string) *Rich {
	r.Path = r.Path.Child(name)
	return r
}
