package wire

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

type innterStruct struct {
	ID   int    `yson:"id"`
	Name string `yson:"name"`
}

type EmbeddedStruct struct {
	EmbeddedID int `yson:"embedded_id"`
}

type EmbeddedStructPtr struct {
	EmbeddedName string `yson:"embedded_name"`
}

type unexportedEmbedded struct {
	ExportedField int64 `yson:"exported_field_of_embedded"`
}

type unexportedEmbeddedPtr struct {
	ExportedField uint64 `yson:"exported_field_of_embedded_ptr"`
}

type TaggedEmbedded struct {
	ExportedField uint `yson:"exported_field_of_tagged_embedded"`
}

type TaggedEmbeddedPtr struct {
	ExportedField bool `yson:"exported_field_of_tagged_embedded_ptr"`
}

type MyString string
type MyInt int

type testItem struct {
	I       int     `yson:"i"`
	I64     int64   `yson:"i64"`
	I32     int32   `yson:"i32"`
	I16     int16   `yson:"i16"`
	I8      int8    `yson:"i8"`
	U       uint    `yson:"u"`
	U64     uint64  `yson:"u64"`
	U32     uint32  `yson:"u32"`
	U16     uint16  `yson:"u16"`
	U8      uint8   `yson:"u8"`
	F32     float32 `yson:"f32"`
	F64     float64 `yson:"f64"`
	Boolean bool    `yson:"bool"`
	String  string  `yson:"string"`
	Bytes   []byte  `yson:"bytes"`

	MyString MyString `yson:"my_string"`
	MyInt    MyInt    `yson:"my_int"`

	Struct    innterStruct  `yson:"struct"`
	StructPtr *innterStruct `yson:"struct_ptr"`

	EmbeddedStruct
	*EmbeddedStructPtr

	unexportedEmbedded
	*unexportedEmbeddedPtr

	TaggedEmbedded     `yson:"tagged"`
	*TaggedEmbeddedPtr `yson:"tagged_ptr"`

	UntaggedExportedField int

	ZeroNum        int `yson:"zero_num"`
	OmittedZeroNum int `yson:"omitted_zero_num,omitempty"`
}

func TestEncode(t *testing.T) {
	for _, tc := range []struct {
		name              string
		items             []any
		expectedNameTable NameTable
		expectedRows      []Row
	}{
		{
			name: "struct",
			items: []any{
				&testItem{
					I:        -1,
					I64:      -64,
					I32:      -32,
					I16:      -16,
					I8:       -8,
					U:        1,
					U64:      64,
					U32:      32,
					U16:      16,
					U8:       8,
					Boolean:  true,
					F32:      32.0,
					F64:      64.0,
					String:   "hello",
					Bytes:    []byte("world"),
					MyString: "my-string",
					MyInt:    1337,
					Struct: innterStruct{
						ID:   88,
						Name: "foo",
					},
					StructPtr: &innterStruct{
						ID:   89,
						Name: "bar",
					},
					EmbeddedStruct: EmbeddedStruct{
						EmbeddedID: 90,
					},
					EmbeddedStructPtr: &EmbeddedStructPtr{
						EmbeddedName: "baz",
					},
					unexportedEmbedded: unexportedEmbedded{
						ExportedField: 91,
					},
					unexportedEmbeddedPtr: &unexportedEmbeddedPtr{
						ExportedField: 92,
					},
					TaggedEmbedded: TaggedEmbedded{
						ExportedField: 93,
					},
					TaggedEmbeddedPtr: &TaggedEmbeddedPtr{
						ExportedField: true,
					},
					UntaggedExportedField: 94,
					ZeroNum:               0,
					OmittedZeroNum:        0,
				},
			},
			expectedNameTable: NameTable{
				{Name: "i"},
				{Name: "i64"},
				{Name: "i32"},
				{Name: "i16"},
				{Name: "i8"},
				{Name: "u"},
				{Name: "u64"},
				{Name: "u32"},
				{Name: "u16"},
				{Name: "u8"},
				{Name: "f32"},
				{Name: "f64"},
				{Name: "bool"},
				{Name: "string"},
				{Name: "bytes"},
				{Name: "my_string"},
				{Name: "my_int"},
				{Name: "struct"},
				{Name: "struct_ptr"},
				{Name: "embedded_id"},
				{Name: "embedded_name"},
				{Name: "exported_field_of_embedded"},
				{Name: "exported_field_of_embedded_ptr"},
				{Name: "tagged"},
				{Name: "tagged_ptr"},
				{Name: "UntaggedExportedField"},
				{Name: "zero_num"},
			},
			expectedRows: []Row{
				{
					NewInt64(0, -1),
					NewInt64(1, -64),
					NewInt64(2, -32),
					NewInt64(3, -16),
					NewInt64(4, -8),
					NewUint64(5, 1),
					NewUint64(6, 64),
					NewUint64(7, 32),
					NewUint64(8, 16),
					NewUint64(9, 8),
					NewFloat64(10, 32.0),
					NewFloat64(11, 64.0),
					NewBool(12, true),
					NewBytes(13, []byte("hello")),
					NewBytes(14, []byte("world")),
					NewBytes(15, []byte("my-string")),
					NewInt64(16, 1337),
					NewAny(17, []byte(`{id=88;name=foo;}`)),
					NewAny(18, []byte(`{id=89;name=bar;}`)),
					NewInt64(19, 90),
					NewBytes(20, []byte("baz")),
					NewInt64(21, 91),
					NewUint64(22, 92),
					NewAny(23, []byte(`{"exported_field_of_tagged_embedded"=93u;}`)),
					NewAny(24, []byte(`{"exported_field_of_tagged_embedded_ptr"=%true;}`)),
					NewInt64(25, 94),
					NewInt64(26, 0),
				},
			},
		},
		{
			name: "map",
			items: []any{
				map[string]any{
					"i":         -1,
					"i64":       int64(-64),
					"i32":       int32(-32),
					"i16":       int16(-16),
					"i8":        int8(-8),
					"u":         uint(1),
					"u64":       uint64(64),
					"u32":       uint32(32),
					"u16":       uint16(16),
					"u8":        uint8(8),
					"f32":       float32(32.0),
					"f64":       64.0,
					"bool":      true,
					"string":    "hello",
					"bytes":     []byte("world"),
					"my_string": MyString("my-string"),
					"my_int":    MyInt(1337),
					"struct": innterStruct{
						ID:   88,
						Name: "foo",
					},
					"struct_ptr": &innterStruct{
						ID:   89,
						Name: "bar",
					},
				},
			},
			expectedNameTable: NameTable{
				{Name: "i"},
				{Name: "i64"},
				{Name: "i32"},
				{Name: "i16"},
				{Name: "i8"},
				{Name: "u"},
				{Name: "u64"},
				{Name: "u32"},
				{Name: "u16"},
				{Name: "u8"},
				{Name: "f32"},
				{Name: "f64"},
				{Name: "bool"},
				{Name: "string"},
				{Name: "bytes"},
				{Name: "my_string"},
				{Name: "my_int"},
				{Name: "struct"},
				{Name: "struct_ptr"},
			},
			expectedRows: []Row{
				{
					NewInt64(0, -1),
					NewInt64(1, -64),
					NewInt64(2, -32),
					NewInt64(3, -16),
					NewInt64(4, -8),
					NewUint64(5, 1),
					NewUint64(6, 64),
					NewUint64(7, 32),
					NewUint64(8, 16),
					NewUint64(9, 8),
					NewFloat64(10, 32.0),
					NewFloat64(11, 64.0),
					NewBool(12, true),
					NewBytes(13, []byte("hello")),
					NewBytes(14, []byte("world")),
					NewBytes(15, []byte("my-string")),
					NewInt64(16, 1337),
					NewAny(17, []byte(`{id=88;name=foo;}`)),
					NewAny(18, []byte(`{id=89;name=bar;}`)),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nameTable, rows, err := Encode(tc.items)
			require.NoError(t, err)

			compareEncodedItems(t,
				EncodeResult{NameTable: tc.expectedNameTable, Rows: tc.expectedRows},
				EncodeResult{NameTable: nameTable, Rows: rows},
			)
		})
	}
}

func TestEncodePivotKeys(t *testing.T) {
	keys := []any{
		[]any{},
		[]string{"a", "b"},
		[]uint64{13, 37},
		[]any{"a", 42},
	}

	expectedRows := []Row{
		make(Row, 0),
		{
			NewBytes(0, []byte("a")),
			NewBytes(0, []byte("b")),
		},
		{
			NewUint64(0, 13),
			NewUint64(0, 37),
		},
		{
			NewBytes(0, []byte("a")),
			NewInt64(0, 42),
		},
	}

	rows, err := EncodePivotKeys(keys)
	require.NoError(t, err)

	require.Equal(t, expectedRows, rows)
}

type EncodeResult struct {
	NameTable NameTable
	Rows      []Row
}

func compareEncodedItems(t *testing.T, expected, actual EncodeResult) {
	t.Helper()

	require.Equal(t, len(expected.Rows), len(actual.Rows))

	for i := 0; i < len(expected.Rows); i++ {
		require.Equal(t,
			makeValueMap(expected.NameTable, expected.Rows[i]),
			makeValueMap(actual.NameTable, actual.Rows[i]),
		)
	}

	compareNameTables(t, expected.NameTable, actual.NameTable)
}

func makeValueMap(table NameTable, row Row) map[string]Value {
	m := make(map[string]Value)
	for i, v := range row {
		v.ID = 0
		m[table[i].Name] = v
	}
	return m
}

func compareNameTables(t *testing.T, expected, actual NameTable) {
	t.Helper()

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Name < expected[j].Name
	})

	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Name < actual[j].Name
	})

	require.Equal(t, expected, actual)
}
