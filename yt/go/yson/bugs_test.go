package yson_test

import (
	"encoding"
	"encoding/json"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
)

type (
	NullTestVal struct {
		Val int `json:"val,omitempty" yson:"val,omitempty"`
	}

	NullTest struct {
		Val    *NullTestVal      `json:"val,omitempty" yson:"val,omitempty"`
		Int    *int              `json:"int,omitempty" yson:"int,omitempty"`
		Map    map[string]any    `json:"map,omitempty" yson:"map,omitempty"`
		MapStr map[string]string `json:"map_str,omitempty" yson:"map_str,omitempty"`
		Array  []string          `json:"array,omitempty" yson:"array,omitempty"`
		Str    string            `json:"str,omitempty" yson:"str,omitempty"`
		Bytes  []byte            `json:"bytes,omitempty" yson:"bytes,omitempty"`
	}
)

func TestNullHandling(t *testing.T) {
	var jsonTest NullTest
	msgJSON := `{"val": null, "int": null, "map": null, "map_str": null, "array": null, "str": null, "bytes": null}`
	require.NoError(t, json.Unmarshal([]byte(msgJSON), &jsonTest))

	var ysonTest NullTest
	msgYSON := `{val=#;int=#;map=#;map_str=#;array=#;str=#;bytes=#}`
	require.NoError(t, yson.Unmarshal([]byte(msgYSON), &ysonTest))

	require.Equal(t, jsonTest, ysonTest)

	{
		s := &NullTest{}
		err := json.Unmarshal([]byte(`null`), &s)
		require.NoError(t, err)
	}

	{
		s := &NullTest{}
		err := yson.Unmarshal([]byte(`#`), &s)
		require.NoError(t, err)
	}
}

type (
	MyInt      int
	MyUint     uint
	MyInt8     int8
	MyUint8    uint8
	MyIntSlice []int
	MyArray    [4]int
	MyString   string
	MyMap      map[string][]int

	TypedefTest struct {
		I   MyInt
		U   MyUint
		I8  MyInt8
		UI8 MyUint8
		II  MyIntSlice
		A   MyArray
		S   MyString
		M   MyMap
	}
)

func TestTypedefTypes(t *testing.T) {
	in := TypedefTest{
		I:   1337,
		U:   1234,
		I8:  -7,
		UI8: 7,
		II:  []int{1, 3, 3, 7},
		A:   [4]int{1, 3, 3, 7},
		S:   "foobar",
		M:   map[string][]int{"e": {1, 3, 3, 7}, "1": {2, 3}},
	}

	var out TypedefTest

	js, err := json.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(js, &out))
	require.Equal(t, in, out)

	ys, err := yson.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, yson.Unmarshal(ys, &out))
	require.Equal(t, in, out)
}

func TestMapKeys(t *testing.T) {
	in := map[MyString]int{
		"foo": 1,
		"bar": 2,
	}
	var out map[MyString]int

	ys, err := yson.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, yson.Unmarshal(ys, &out))
	require.Equal(t, in, out)
}

func TestFieldEmbedding(t *testing.T) {
	type makeInOut func() (any, any)

	for _, tc := range []struct {
		name   string
		init   makeInOut
		js, ys string
	}{
		{
			name: "untagged",
			init: func() (in any, out any) {
				type I1 struct {
					ID int
				}

				type I2 struct {
					ID2 int
				}

				type S struct {
					I1
					*I2
				}

				in = &S{
					I1: I1{ID: 10},
					I2: &I2{ID2: 20},
				}
				out = &S{}

				return
			},
			js: `{"ID":10,"ID2":20}`,
			ys: `{ID=10;ID2=20;}`,
		},
		{
			name: "untagged_nil",
			init: func() (in any, out any) {
				type I struct {
					ID int
				}

				type S struct {
					*I
				}

				return &S{I: nil}, &S{}
			},
			js: `{}`,
			ys: `{}`,
		},
		{
			name: "untagged_basic_type",
			init: func() (in any, out any) {
				type ID = int

				type S struct {
					A int `yson:"a" json:"a"`
					ID
				}

				return &S{A: 10, ID: 20}, &S{}
			},
			js: `{"a":10,"ID":20}`,
			ys: `{a=10;ID=20;}`,
		},
		{
			name: "tagged_basic_type",
			init: func() (in any, out any) {
				type ID = int

				type S struct {
					A   int `yson:"a" json:"a"`
					*ID `yson:"id" json:"id"`
				}

				id := 20
				return &S{A: 10, ID: &id}, &S{}
			},
			js: `{"a":10,"id":20}`,
			ys: `{a=10;id=20;}`,
		},
		{
			name: "untagged_declaration",
			init: func() (in any, out any) {
				type ID string

				type S struct {
					A int `yson:"a" json:"a"`
					ID
				}

				id := ID("str")
				return &S{A: 10, ID: id}, &S{}
			},
			js: `{"a":10,"ID":"str"}`,
			ys: `{a=10;ID=str;}`,
		},
		{
			name: "tagged_declaration",
			init: func() (in any, out any) {
				type ID string

				type S struct {
					A   int `yson:"a" json:"a"`
					*ID `yson:"id" json:"id"`
				}

				id := ID("str")
				return &S{A: 10, ID: &id}, &S{}
			},
			js: `{"a":10,"id":"str"}`,
			ys: `{a=10;id=str;}`,
		},
		{
			name: "untagged_unexported",
			init: func() (in any, out any) {
				type i struct {
					ID string `yson:"id" json:"id"`
				}

				type S struct {
					i
				}

				return &S{i: i{ID: "str"}}, &S{}
			},
			js: `{"id":"str"}`,
			ys: `{id=str;}`,
		},
		{
			name: "untagged_unexported_ptr",
			init: func() (in any, out any) {
				type i struct {
					ID string `yson:"id" json:"id"`
				}

				type S struct {
					*i
				}

				return &S{i: &i{ID: "str"}}, &S{i: &i{}}
			},
			js: `{"id":"str"}`,
			ys: `{id=str;}`,
		},
		{
			name: "embedded_value",
			init: func() (in any, out any) {
				type I struct {
					ID string `yson:"id" json:"id"`
				}

				type S struct {
					*I `yson:"i" json:"i"`
				}

				return &S{I: &I{ID: "str"}}, &S{}
			},
			js: `{"i":{"id":"str"}}`,
			ys: `{i={id=str;};}`,
		},
		{
			name: "embedded_attr",
			init: func() (in any, out any) {
				type I struct {
					ID string `yson:"id" json:"id"`
				}

				type S struct {
					I `yson:"i,attr" json:"i"`
				}

				return &S{I: I{ID: "str"}}, &S{}
			},
			js: `{"i":{"id":"str"}}`,
			ys: `<i={id=str;};>{}`,
		},
		{
			name: "attr_of_embedded_attr",
			init: func() (in any, out any) {
				type I struct {
					ID string `yson:"id,attr" json:"id"`
				}

				type S struct {
					I `yson:"i,attr" json:"i"`
				}

				return &S{I: I{ID: "str"}}, &S{}
			},
			js: `{"i":{"id":"str"}}`,
			ys: `<i=<id=str;>{};>{}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			{
				in, out := tc.init()
				js, err := json.Marshal(in)
				require.NoError(t, err)
				require.Equal(t, tc.js, string(js))

				require.NoError(t, json.Unmarshal(js, out))
				require.Equal(t, in, out)
			}

			{
				in, out := tc.init()
				ys, err := yson.MarshalFormat(in, yson.FormatText)
				require.NoError(t, err)
				require.Equal(t, tc.ys, string(ys))

				require.NoError(t, yson.Unmarshal(ys, out))
				require.Equal(t, in, out)
			}
		})
	}
}

func TestSkippedEmbeddedField(t *testing.T) {
	type I struct {
		ID string `yson:"id" json:"id"`
	}

	type S struct {
		*I `yson:"-" json:"-"`
	}

	{
		in := &S{I: &I{ID: "str"}}
		js, err := json.Marshal(in)
		require.NoError(t, err)
		require.Equal(t, `{}`, string(js))

		out := &S{}
		err = json.Unmarshal([]byte(`{"id":"str"}`), out)
		require.NoError(t, err)
		require.Equal(t, &S{}, out)
	}

	{
		in := &S{I: &I{ID: "str"}}
		ys, err := yson.MarshalFormat(in, yson.FormatText)
		require.NoError(t, err)
		require.Equal(t, `{}`, string(ys))

		out := &S{}
		err = yson.Unmarshal([]byte(`{id=str;}`), out)
		require.NoError(t, err)
		require.Equal(t, &S{}, out)
	}
}

type (
	Unexported struct {
		A int
		b string
		c bool
	}
)

func TestUnexportedFields(t *testing.T) {
	in := Unexported{A: 10, b: "foo", c: true}

	js, err := yson.MarshalFormat(in, yson.FormatText)
	require.NoError(t, err)
	require.Equal(t, `{A=10;}`, string(js))

	var out Unexported
	require.NoError(t, yson.Unmarshal([]byte("{A=10;b=foo}"), &out))

	require.Equal(t, Unexported{A: 10}, out)
}

type (
	RecursiveA struct {
		*RecursiveB
	}

	RecursiveB struct {
		*RecursiveA
	}
)

func TestRecursiveTypes(t *testing.T) {
	t.Skipf("this is broken")

	in := RecursiveA{&RecursiveB{&RecursiveA{&RecursiveB{}}}}
	expected := `{}`

	ys, err := yson.Marshal(in)
	require.NoError(t, err)
	require.Equal(t, expected, string(ys))

	var out RecursiveA
	require.NoError(t, yson.Unmarshal(ys, &out))

	require.Equal(t, in, out)
}

func TestInvalidSkip(t *testing.T) {
	var row struct {
		Name  string
		Value [][]any
	}
	row.Name = "foo"
	row.Value = [][]any{
		{1, 2, 0.1},
		{3, 4, 0.5},
	}

	ys, err := yson.MarshalFormat(row, yson.FormatBinary)
	require.NoError(t, err)

	require.NoError(t, yson.Unmarshal(ys, &row))
}

func TestAttrsUnmarshal(t *testing.T) {
	t.Skip("This is broken")

	var result struct {
		Type string `yson:"type,attr"`
	}

	in := `<type=foo>#`
	require.NoError(t, yson.Unmarshal([]byte(in), &result))

	in = `#`
	require.NoError(t, yson.Unmarshal([]byte(in), &result))
}

type (
	ysonBeforeBinary struct{}
	textBeforeBinary struct{}
)

func (y *ysonBeforeBinary) UnmarshalBinary(data []byte) error {
	panic("this should never be called")
}

func (y *ysonBeforeBinary) MarshalBinary() (data []byte, err error) {
	panic("this should never be called")
}

func (y *ysonBeforeBinary) UnmarshalYSON(r *yson.Reader) error {
	_, err := r.NextRawValue()
	return err
}

func (y *ysonBeforeBinary) MarshalYSON(w *yson.Writer) error {
	w.Entity()
	return w.Err()
}

func (b *textBeforeBinary) UnmarshalText(text []byte) error {
	return nil
}

func (b *textBeforeBinary) MarshalText() (text []byte, err error) {
	return nil, nil
}

func (b *textBeforeBinary) UnmarshalBinary(data []byte) error {
	panic("this should never be called")
}

func (b *textBeforeBinary) MarshalBinary() (data []byte, err error) {
	panic("this should never be called")
}

var (
	_ yson.StreamMarshaler       = &ysonBeforeBinary{}
	_ yson.StreamUnmarshaler     = &ysonBeforeBinary{}
	_ encoding.BinaryMarshaler   = &ysonBeforeBinary{}
	_ encoding.BinaryUnmarshaler = &ysonBeforeBinary{}

	_ encoding.BinaryMarshaler   = &textBeforeBinary{}
	_ encoding.BinaryUnmarshaler = &textBeforeBinary{}
	_ encoding.TextMarshaler     = &textBeforeBinary{}
	_ encoding.TextUnmarshaler   = &textBeforeBinary{}
)

func TestHookPriority(t *testing.T) {
	for _, v := range []any{
		&ysonBeforeBinary{},
		&textBeforeBinary{},
	} {
		ys, err := yson.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, yson.Unmarshal(ys, v))
	}
}

type User struct {
	ID *uuid.UUID
}

func TestNilPointerMarshal(t *testing.T) {
	var u User

	_, err := json.Marshal(u)
	require.NoError(t, err)

	_, err = yson.Marshal(u)
	require.NoError(t, err)
}

func TestNilPointerUnmarshal(t *testing.T) {
	id, err := uuid.NewV4()
	require.NoError(t, err)

	u := User{ID: &id}

	{
		var u2 User

		js, err := json.Marshal(u)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(js, &u2))
	}

	{
		var u2 User

		ys, err := yson.Marshal(u)
		require.NoError(t, err)
		require.NoError(t, yson.Unmarshal(ys, &u2))
	}
}

func TestUnmarshalEmbeddedUnexportedNilPtr(t *testing.T) {
	type i struct {
		ID string `yson:"id" json:"id"`
	}

	type S struct {
		*i
	}

	{
		s := &S{}
		err := json.Unmarshal([]byte(`{"id":"str"}`), s)
		require.Error(t, err)
	}

	{
		s := &S{}
		err := yson.Unmarshal([]byte(`{id=str;}`), s)
		require.Error(t, err)
	}
}

func TestUnmarshalEmbeddedUnexportedNonStruct(t *testing.T) {
	type i any
	type S struct {
		i
	}

	{
		s := &S{}
		err := yson.Unmarshal([]byte(`{i=4;}`), s)
		require.NoError(t, err)
	}
}

func TestJSONNumberConversion(t *testing.T) {
	type S struct {
		I int64
		U uint64
		F float64
	}

	for _, testCase := range []struct {
		JSON string
		Err  bool
	}{
		{JSON: `{"I":10}`},
		{JSON: `{"I":18446744073709551616}`, Err: true},
		{JSON: `{"U":18446744073709551616}`, Err: true},
		{JSON: `{"U":-1}`, Err: true},
		{JSON: `{"U":18446744073709551615}`},
		{JSON: `{"U":0.5}`, Err: true},
		{JSON: `{"I":0.5}`, Err: true},
		{JSON: `{"U":1.0}`, Err: true},
		{JSON: `{"I":1.0}`, Err: true},
		{JSON: `{"I":1e2}`, Err: true},
		{JSON: `{"U":1e2}`, Err: true},
		{JSON: `{"F":18446744073709551615}`},
		{JSON: `{"F":1e-3000}`},
	} {
		t.Run(testCase.JSON, func(t *testing.T) {
			var v S
			if testCase.Err {
				require.Error(t, json.Unmarshal([]byte(testCase.JSON), &v))
			} else {
				require.NoError(t, json.Unmarshal([]byte(testCase.JSON), &v))
			}
		})
	}
}

func TestYSONNumberConversion(t *testing.T) {
	type S struct {
		I int64
		U uint64
		F float64
	}

	for _, testCase := range []struct {
		YSON string
		Err  bool
	}{
		{YSON: `{I=10}`},
		{YSON: `{I=10u}`},
		{YSON: `{U=10}`},
		{YSON: `{I=18446744073709551616}`, Err: true},
		{YSON: `{U=18446744073709551616u}`, Err: true},
		{YSON: `{U=-1}`, Err: true},
		{YSON: `{U=18446744073709551615u}`},
		{YSON: `{U=0.5}`, Err: true},
		{YSON: `{I=0.5}`, Err: true},
		{YSON: `{U=1.0}`, Err: true},
		{YSON: `{I=1.0}`, Err: true},
		{YSON: `{I=1e2}`, Err: true},
		{YSON: `{U=1e2}`, Err: true},
		{YSON: `{F=18446744073709551615u}`},
		{YSON: `{F=-1}`},
		{YSON: `{F=1e-3000}`},
		{YSON: `{F=1e2}`},
	} {
		t.Run(testCase.YSON, func(t *testing.T) {
			var v S
			if testCase.Err {
				require.Error(t, yson.Unmarshal([]byte(testCase.YSON), &v))
			} else {
				require.NoError(t, yson.Unmarshal([]byte(testCase.YSON), &v))
			}
		})
	}
}

func Test8BitIntegerSlices(t *testing.T) {
	i := []int8{1, 2, 3}
	u := []uint8{1, 2, 3}
	b := []byte{1, 2, 3}

	{
		js, err := json.Marshal(i)
		require.NoError(t, err)
		require.Equal(t, []byte("[1,2,3]"), js)

		var ii []int8
		require.NoError(t, json.Unmarshal(js, &ii))
		require.Equal(t, i, ii)

		js, err = json.Marshal(u)
		require.NoError(t, err)
		require.Equal(t, []byte(`"AQID"`), js)

		var uu []uint8
		require.NoError(t, json.Unmarshal(js, &uu))
		require.Equal(t, u, uu)

		js, err = json.Marshal(b)
		require.NoError(t, err)
		require.Equal(t, []byte(`"AQID"`), js)

		var bb []byte
		require.NoError(t, json.Unmarshal(js, &bb))
		require.Equal(t, b, bb)
	}

	{
		ys, err := yson.Marshal(i)
		require.NoError(t, err)
		require.Equal(t, []byte("[1;2;3;]"), ys)

		var ii []int8
		require.NoError(t, yson.Unmarshal(ys, &ii))
		require.Equal(t, i, ii)

		ys, err = yson.Marshal(u)
		require.NoError(t, err)
		require.Equal(t, []byte(`"\1\2\3"`), ys)

		var uu []uint8
		require.NoError(t, yson.Unmarshal(ys, &uu))
		require.Equal(t, u, uu)

		ys, err = yson.Marshal(b)
		require.NoError(t, err)
		require.Equal(t, []byte(`"\1\2\3"`), ys)

		var bb []byte
		require.NoError(t, yson.Unmarshal(ys, &bb))
		require.Equal(t, b, bb)
	}
}
