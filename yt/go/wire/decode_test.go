package wire

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type testResponseRow struct {
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

	Struct    innterStruct  `yson:"struct"`
	StructPtr *innterStruct `yson:"struct_ptr"`

	EmbeddedStruct
	*EmbeddedStructPtr

	unexportedEmbedded
	// NOTE: Unexported embedded struct pointers are not supported.

	TaggedEmbedded     `yson:"tagged"`
	*TaggedEmbeddedPtr `yson:"tagged_ptr"`
}

type structWithUnexportedEmbeddedStructPointer struct {
	*unexportedEmbeddedPtr
}

func TestDecoder_UnmarshalRow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		nameTable NameTable
		in        Row
		expected  interface{}
		isErr     bool
	}{
		{
			name: "struct",
			nameTable: NameTable{
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
				{Name: "struct"},
				{Name: "struct_ptr"},
				{Name: "embedded_id"},
				{Name: "embedded_name"},
				{Name: "exported_field_of_embedded"},
				{Name: "tagged"},
				{Name: "tagged_ptr"},
			},
			in: Row{
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
				NewAny(15, []byte(`{id=88;name=foo;}`)),
				NewAny(16, []byte(`{id=89;name=bar;}`)),
				NewInt64(17, 90),
				NewBytes(18, []byte("baz")),
				NewInt64(19, 91),
				NewAny(20, []byte(`{"exported_field_of_tagged_embedded"=93u;}`)),
				NewAny(21, []byte(`{"exported_field_of_tagged_embedded_ptr"=%true;}`)),
			},
			expected: &testResponseRow{
				I:       -1,
				I64:     -64,
				I32:     -32,
				I16:     -16,
				I8:      -8,
				U:       1,
				U64:     64,
				U32:     32,
				U16:     16,
				U8:      8,
				F32:     32.0,
				F64:     64.0,
				Boolean: true,
				String:  "hello",
				Bytes:   []byte("world"),
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
				TaggedEmbedded: TaggedEmbedded{
					ExportedField: 93,
				},
				TaggedEmbeddedPtr: &TaggedEmbeddedPtr{
					ExportedField: true,
				},
			},
		},
		{
			name: "map",
			nameTable: NameTable{
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
				{Name: "struct"},
				{Name: "struct_ptr"},
			},
			in: Row{
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
				NewAny(15, []byte(`{id=88;name=foo;}`)),
				NewAny(16, []byte(`{id=89;name=bar;}`)),
			},
			expected: &map[string]interface{}{
				"i":      int64(-1),
				"i64":    int64(-64),
				"i32":    int64(-32),
				"i16":    int64(-16),
				"i8":     int64(-8),
				"u":      uint64(1),
				"u64":    uint64(64),
				"u32":    uint64(32),
				"u16":    uint64(16),
				"u8":     uint64(8),
				"f32":    32.0,
				"f64":    64.0,
				"bool":   true,
				"string": []byte("hello"),
				"bytes":  []byte("world"),
				"struct": map[string]interface{}{
					"id":   int64(88),
					"name": "foo",
				},
				"struct_ptr": map[string]interface{}{
					"id":   int64(89),
					"name": "bar",
				},
			},
		},
		{
			name: "int8_overflow",
			nameTable: NameTable{
				{Name: "i8"},
			},
			in:       Row{Value{ID: 0, Type: TypeInt64, scalar: math.MaxInt8 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "int16_overflow",
			nameTable: NameTable{
				{Name: "i16"},
			},
			in:       Row{Value{ID: 0, Type: TypeInt64, scalar: math.MaxInt16 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "int32_overflow",
			nameTable: NameTable{
				{Name: "i32"},
			},
			in:       Row{Value{ID: 0, Type: TypeInt64, scalar: math.MaxInt32 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "uint8_overflow",
			nameTable: NameTable{
				{Name: "u8"},
			},
			in:       Row{Value{ID: 0, Type: TypeUint64, scalar: math.MaxUint8 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "uint16_overflow",
			nameTable: NameTable{
				{Name: "u16"},
			},
			in:       Row{Value{ID: 0, Type: TypeUint64, scalar: math.MaxUint16 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "uint32_overflow",
			nameTable: NameTable{
				{Name: "u32"},
			},
			in:       Row{Value{ID: 0, Type: TypeUint64, scalar: math.MaxUint32 + 1}},
			expected: &testResponseRow{},
			isErr:    true,
		},
		{
			name: "unexported_embedded_struct_ptr",
			nameTable: NameTable{
				{Name: "exported_field_of_embedded_ptr"},
			},
			in: Row{
				NewUint64(0, 42),
			},
			expected: &structWithUnexportedEmbeddedStructPointer{
				unexportedEmbeddedPtr: &unexportedEmbeddedPtr{
					ExportedField: 42,
				},
			},
			isErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(tc.nameTable)

			out := reflect.New(reflect.TypeOf(tc.expected).Elem()).Interface()
			err := d.UnmarshalRow(tc.in, out)

			if !tc.isErr {
				require.NoError(t, err)
				require.Equal(t, tc.expected, out)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDecoder_nilRow(t *testing.T) {
	d := NewDecoder(nil)
	var out interface{}
	require.NoError(t, d.UnmarshalRow(nil, &out))
}
