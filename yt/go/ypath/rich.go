package ypath

import "a.yandex-team.ru/yt/go/yson"

type Key []interface{}

type ReadLimit struct {
	Key      []interface{} `yson:"key,omitempty"`
	RowIndex *int64        `yson:"row_index,omitempty"`
}

type Range struct {
	Lower *ReadLimit `yson:"lower_limit,omitempty"`
	Upper *ReadLimit `yson:"lower_limit,omitempty"`
	Exact *ReadLimit `yson:"lower_limit,omitempty"`
}

type Rich struct {
	Path Path `yson:",value"`

	Append   *bool    `yson:"append,attr,omitempty"`
	SortedBy []string `yson:"sorted_by,attr,omitempty"`
	Ranges   []Range  `yson:"ranges,attr,omitempty"`

	Compression  string `yson:"compression,attr,omitempty"`
	ErasureCodec string `yson:"erasure_codec,attr,omitempty"`
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

func (p Rich) AppendName(name string) Rich {
	p.Path += Path("/" + name)
	return p
}
