package wire

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
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
	I         int              `yson:"i"`
	I64       int64            `yson:"i64"`
	I32       int32            `yson:"i32"`
	I16       int16            `yson:"i16"`
	I8        int8             `yson:"i8"`
	U         uint             `yson:"u"`
	U64       uint64           `yson:"u64"`
	U32       uint32           `yson:"u32"`
	U16       uint16           `yson:"u16"`
	U8        uint8            `yson:"u8"`
	F32       float32          `yson:"f32"`
	F64       float64          `yson:"f64"`
	Boolean   bool             `yson:"bool"`
	String    string           `yson:"string"`
	Bytes     []byte           `yson:"bytes"`
	Date      schema.Date      `yson:"date"`
	Datetime  schema.Datetime  `yson:"datetime"`
	Timestamp schema.Timestamp `yson:"timestamp"`
	Interval  schema.Interval  `yson:"interval"`

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

	I64Ptr    *int64 `yson:"i64_ptr"`
	I64NilPtr *int64 `yson:"i64_nil_ptr"`
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
					I:         -1,
					I64:       -64,
					I32:       -32,
					I16:       -16,
					I8:        -8,
					U:         1,
					U64:       64,
					U32:       32,
					U16:       16,
					U8:        8,
					Boolean:   true,
					F32:       32.0,
					F64:       64.0,
					String:    "hello",
					Bytes:     []byte("world"),
					Date:      schema.Date(1),
					Datetime:  schema.Datetime(2),
					Timestamp: schema.Timestamp(3),
					Interval:  schema.Interval(4),
					MyString:  "my-string",
					MyInt:     1337,
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
					I64Ptr:                ptr.Int64(42),
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
				{Name: "date"},
				{Name: "datetime"},
				{Name: "timestamp"},
				{Name: "interval"},
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
				{Name: "i64_ptr"},
				{Name: "i64_nil_ptr"},
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
					NewUint64(15, 1),
					NewUint64(16, 2),
					NewUint64(17, 3),
					NewInt64(18, 4),
					NewBytes(19, []byte("my-string")),
					NewInt64(20, 1337),
					NewAny(21, []byte{0x7b, 0x1, 0x4, 0x69, 0x64, 0x3d, 0x2, 0xb0, 0x1, 0x3b, 0x1, 0x8, 0x6e, 0x61, 0x6d, 0x65, 0x3d, 0x1, 0x6, 0x66, 0x6f, 0x6f, 0x3b, 0x7d}),
					NewAny(22, []byte{0x7b, 0x1, 0x4, 0x69, 0x64, 0x3d, 0x2, 0xb2, 0x1, 0x3b, 0x1, 0x8, 0x6e, 0x61, 0x6d, 0x65, 0x3d, 0x1, 0x6, 0x62, 0x61, 0x72, 0x3b, 0x7d}),
					NewInt64(23, 90),
					NewBytes(24, []byte("baz")),
					NewInt64(25, 91),
					NewUint64(26, 92),
					NewAny(27, []byte{0x7b, 0x1, 0x42, 0x65, 0x78, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x5f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6f, 0x66, 0x5f, 0x74, 0x61, 0x67, 0x67, 0x65, 0x64, 0x5f, 0x65, 0x6d, 0x62, 0x65, 0x64, 0x64, 0x65, 0x64, 0x3d, 0x6, 0x5d, 0x3b, 0x7d}),
					NewAny(28, []byte{0x7b, 0x1, 0x4a, 0x65, 0x78, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x5f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6f, 0x66, 0x5f, 0x74, 0x61, 0x67, 0x67, 0x65, 0x64, 0x5f, 0x65, 0x6d, 0x62, 0x65, 0x64, 0x64, 0x65, 0x64, 0x5f, 0x70, 0x74, 0x72, 0x3d, 0x5, 0x3b, 0x7d}),
					NewInt64(29, 94),
					NewInt64(30, 0),
					NewInt64(31, 42),
					NewNull(32),
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
					"date":      schema.Date(1),
					"datetime":  schema.Datetime(2),
					"timestamp": schema.Timestamp(3),
					"interval":  schema.Interval(4),
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
				{Name: "date"},
				{Name: "datetime"},
				{Name: "timestamp"},
				{Name: "interval"},
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
					NewUint64(15, 1),
					NewUint64(16, 2),
					NewUint64(17, 3),
					NewInt64(18, 4),
					NewBytes(19, []byte("my-string")),
					NewInt64(20, 1337),
					NewAny(21, []byte{0x7b, 0x1, 0x4, 0x69, 0x64, 0x3d, 0x2, 0xb0, 0x1, 0x3b, 0x1, 0x8, 0x6e, 0x61, 0x6d, 0x65, 0x3d, 0x1, 0x6, 0x66, 0x6f, 0x6f, 0x3b, 0x7d}),
					NewAny(22, []byte{0x7b, 0x1, 0x4, 0x69, 0x64, 0x3d, 0x2, 0xb2, 0x1, 0x3b, 0x1, 0x8, 0x6e, 0x61, 0x6d, 0x65, 0x3d, 0x1, 0x6, 0x62, 0x61, 0x72, 0x3b, 0x7d}),
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
