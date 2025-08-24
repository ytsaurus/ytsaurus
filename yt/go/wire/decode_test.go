package wire

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
)

type testResponseRow struct {
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
		expected  any
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
				NewUint64(15, 1),
				NewUint64(16, 2),
				NewUint64(17, 3),
				NewInt64(18, 4),
				NewBytes(19, []byte("my-string")),
				NewInt64(20, 1337),
				NewAny(21, []byte(`{id=88;name=foo;}`)),
				NewAny(22, []byte(`{id=89;name=bar;}`)),
				NewInt64(23, 90),
				NewBytes(24, []byte("baz")),
				NewInt64(25, 91),
				NewAny(26, []byte(`{"exported_field_of_tagged_embedded"=93u;}`)),
				NewAny(27, []byte(`{"exported_field_of_tagged_embedded_ptr"=%true;}`)),
			},
			expected: &testResponseRow{
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
				F32:       32.0,
				F64:       64.0,
				Boolean:   true,
				String:    "hello",
				Bytes:     []byte("world"),
				Date:      schema.Date(1),
				Datetime:  schema.Datetime(2),
				Timestamp: schema.Timestamp(3),
				Interval:  schema.Interval(4),
				MyString:  MyString("my-string"),
				MyInt:     MyInt(1337),
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
				{Name: "date"},
				{Name: "datetime"},
				{Name: "timestamp"},
				{Name: "interval"},
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
				NewUint64(15, 1),
				NewUint64(16, 2),
				NewUint64(17, 3),
				NewInt64(18, 4),
				NewAny(19, []byte(`{id=88;name=foo;}`)),
				NewAny(20, []byte(`{id=89;name=bar;}`)),
			},
			expected: &map[string]any{
				"i":         int64(-1),
				"i64":       int64(-64),
				"i32":       int64(-32),
				"i16":       int64(-16),
				"i8":        int64(-8),
				"u":         uint64(1),
				"u64":       uint64(64),
				"u32":       uint64(32),
				"u16":       uint64(16),
				"u8":        uint64(8),
				"f32":       32.0,
				"f64":       64.0,
				"bool":      true,
				"string":    []byte("hello"),
				"bytes":     []byte("world"),
				"date":      uint64(1),
				"datetime":  uint64(2),
				"timestamp": uint64(3),
				"interval":  int64(4),
				"struct": map[string]any{
					"id":   int64(88),
					"name": "foo",
				},
				"struct_ptr": map[string]any{
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
		{
			name: "null_string",
			nameTable: NameTable{
				{Name: "string"},
			},
			in:       Row{Value{ID: 0, Type: TypeNull}},
			expected: &testResponseRow{},
		},
		{
			name: "null_int",
			nameTable: NameTable{
				{Name: "i"},
			},
			in:       Row{Value{ID: 0, Type: TypeNull}},
			expected: &testResponseRow{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(tc.nameTable, nil)

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

func TestDecoder_CompositeTypes(t *testing.T) {
	tests := []struct {
		name      string
		schema    *schema.Schema
		nameTable NameTable
		testData  []byte
		expected  any
		isErr     bool
	}{
		{
			name: "struct_simple",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "person",
						ComplexType: schema.Struct{
							Members: []schema.StructMember{
								{Name: "id", Type: schema.TypeInt64},
								{Name: "name", Type: schema.TypeString},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "person"}},
			testData:  []byte(`[123;"alice"]`),
			expected: map[string]any{
				"person": map[string]any{
					"id":   int64(123),
					"name": "alice",
				},
			},
		},
		{
			name: "list_of_structs",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "people",
						ComplexType: schema.List{
							Item: schema.Struct{
								Members: []schema.StructMember{
									{Name: "id", Type: schema.TypeInt64},
									{Name: "name", Type: schema.TypeString},
								},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "people"}},
			testData:  []byte(`[[123;"alice"];[456;"bob"]]`),
			expected: map[string]any{
				"people": []any{
					map[string]any{
						"id":   int64(123),
						"name": "alice",
					},
					map[string]any{
						"id":   int64(456),
						"name": "bob",
					},
				},
			},
		},
		{
			name: "list_of_primitives",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "numbers",
						ComplexType: schema.List{
							Item: schema.TypeInt64,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "numbers"}},
			testData:  []byte(`[1;2;3;4;5]`),
			expected: map[string]any{
				"numbers": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			},
		},
		{
			name: "tuple_simple",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "coordinates",
						ComplexType: schema.Tuple{
							Elements: []schema.TupleElement{
								{Type: schema.TypeInt64},
								{Type: schema.TypeFloat64},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "coordinates"}},
			testData:  []byte(`[10;3.14]`),
			expected: map[string]any{
				"coordinates": []any{int64(10), 3.14},
			},
		},
		{
			name: "tuple_with_structs",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "pair",
						ComplexType: schema.Tuple{
							Elements: []schema.TupleElement{
								{Type: schema.Struct{
									Members: []schema.StructMember{
										{Name: "id", Type: schema.TypeInt64},
									},
								}},
								{Type: schema.Struct{
									Members: []schema.StructMember{
										{Name: "name", Type: schema.TypeString},
									},
								}},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "pair"}},
			testData:  []byte(`[[42];[test]]`),
			expected: map[string]any{
				"pair": []any{
					map[string]any{"id": int64(42)},
					map[string]any{"name": "test"},
				},
			},
		},
		{
			name: "dict_simple",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "config",
						ComplexType: schema.Dict{
							Key:   schema.TypeString,
							Value: schema.TypeInt64,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "config"}},
			testData:  []byte(`[["key1";1];["key2";2]]`),
			expected: map[string]any{
				"config": map[any]any{
					"key1": int64(1),
					"key2": int64(2),
				},
			},
		},
		{
			name: "dict_with_complex_values",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "users",
						ComplexType: schema.Dict{
							Key: schema.TypeString,
							Value: schema.Struct{
								Members: []schema.StructMember{
									{Name: "age", Type: schema.TypeInt64},
									{Name: "active", Type: schema.TypeBoolean},
								},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "users"}},
			testData:  []byte(`[["alice";[25;%true]];["bob";[30;%false]]]`),
			expected: map[string]any{
				"users": map[any]any{
					"alice": map[string]any{"age": int64(25), "active": true},
					"bob":   map[string]any{"age": int64(30), "active": false},
				},
			},
		},
		{
			name: "optional_with_value",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "maybe_string",
						ComplexType: schema.Optional{
							Item: schema.TypeString,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "maybe_string"}},
			testData:  []byte(`"hello"`),
			expected: map[string]any{
				"maybe_string": "hello",
			},
		},
		{
			name: "optional_with_null",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "maybe_string",
						ComplexType: schema.Optional{
							Item: schema.TypeString,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "maybe_string"}},
			testData:  []byte(`#`),
			expected: map[string]any{
				"maybe_string": nil,
			},
		},
		{
			name: "optional_with_struct",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "maybe_person",
						ComplexType: schema.Optional{
							Item: schema.Struct{
								Members: []schema.StructMember{
									{Name: "id", Type: schema.TypeInt64},
									{Name: "name", Type: schema.TypeString},
								},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "maybe_person"}},
			testData:  []byte(`[123;"alice"]`),
			expected: map[string]any{
				"maybe_person": map[string]any{
					"id":   int64(123),
					"name": "alice",
				},
			},
		},
		{
			name: "tagged_simple",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "user_id",
						ComplexType: schema.Tagged{
							Tag:  "user_id",
							Item: schema.TypeInt64,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "user_id"}},
			testData:  []byte(`123`),
			expected: map[string]any{
				"user_id": int64(123),
			},
		},
		{
			name: "tagged_with_struct",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "user_data",
						ComplexType: schema.Tagged{
							Tag: "user_data",
							Item: schema.Struct{
								Members: []schema.StructMember{
									{Name: "id", Type: schema.TypeInt64},
									{Name: "name", Type: schema.TypeString},
								},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "user_data"}},
			testData:  []byte(`[123;"alice"]`),
			expected: map[string]any{
				"user_data": map[string]any{
					"id":   int64(123),
					"name": "alice",
				},
			},
		},
		{
			name: "variant_struct",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "shape",
						ComplexType: schema.Variant{
							Members: []schema.StructMember{
								{Name: "circle", Type: schema.Struct{
									Members: []schema.StructMember{
										{Name: "radius", Type: schema.TypeFloat64},
									},
								}},
								{Name: "rectangle", Type: schema.Struct{
									Members: []schema.StructMember{
										{Name: "width", Type: schema.TypeFloat64},
										{Name: "height", Type: schema.TypeFloat64},
									},
								}},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "shape"}},
			testData:  []byte(`["circle";[5.0]]`),
			expected: map[string]any{
				"shape": []any{"circle", map[string]any{"radius": 5.0}},
			},
		},
		{
			name: "variant_tuple",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "value",
						ComplexType: schema.Variant{
							Elements: []schema.TupleElement{
								{Type: schema.TypeInt64},
								{Type: schema.TypeString},
								{Type: schema.TypeFloat64},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "value"}},
			testData:  []byte(`[1;"hello"]`),
			expected: map[string]any{
				"value": []any{int64(1), "hello"},
			},
		},
		{
			name: "nested_complex_types",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "complex",
						ComplexType: schema.List{
							Item: schema.Optional{
								Item: schema.Dict{
									Key:   schema.TypeString,
									Value: schema.TypeInt64,
								},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "complex"}},
			testData:  []byte(`[[["key1";1]];#;[["key2";2]]]`),
			expected: map[string]any{
				"complex": []any{
					map[any]any{"key1": int64(1)},
					nil,
					map[any]any{"key2": int64(2)},
				},
			},
		},
		{
			name: "decimal_type",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "price",
						ComplexType: schema.Decimal{
							Precision: 10,
							Scale:     2,
						},
					},
				},
			},
			nameTable: NameTable{{Name: "price"}},
			testData:  []byte(`"123.45"`),
			expected: map[string]any{
				"price": "123.45",
			},
		},
		{
			name: "list_of_optional_primitives",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "nullable_numbers",
						ComplexType: schema.List{
							Item: schema.Optional{
								Item: schema.TypeInt64,
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "nullable_numbers"}},
			testData:  []byte(`[1;#;3;#;5]`),
			expected: map[string]any{
				"nullable_numbers": []any{int64(1), nil, int64(3), nil, int64(5)},
			},
		},
		{
			name: "struct_with_optional_fields",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "user",
						ComplexType: schema.Struct{
							Members: []schema.StructMember{
								{Name: "id", Type: schema.TypeInt64},
								{Name: "name", Type: schema.Optional{
									Item: schema.TypeString,
								}},
								{Name: "email", Type: schema.Optional{
									Item: schema.TypeString,
								}},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "user"}},
			testData:  []byte(`[123;"alice";#]`),
			expected: map[string]any{
				"user": map[string]any{
					"id":    int64(123),
					"name":  "alice",
					"email": nil,
				},
			},
		},
		{
			name: "dict_with_list_values",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "categories",
						ComplexType: schema.Dict{
							Key: schema.TypeString,
							Value: schema.List{
								Item: schema.TypeString,
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "categories"}},
			testData:  []byte(`[["fruits";["apple";"banana"]];["vegetables";["carrot";"lettuce"]]]`),
			expected: map[string]any{
				"categories": map[any]any{
					"fruits":     []any{"apple", "banana"},
					"vegetables": []any{"carrot", "lettuce"},
				},
			},
		},
		{
			name: "tuple_with_optional_elements",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "mixed_tuple",
						ComplexType: schema.Tuple{
							Elements: []schema.TupleElement{
								{Type: schema.TypeInt64},
								{Type: schema.Optional{
									Item: schema.TypeString,
								}},
								{Type: schema.TypeFloat64},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "mixed_tuple"}},
			testData:  []byte(`[10;"hello";3.14]`),
			expected: map[string]any{
				"mixed_tuple": []any{int64(10), "hello", 3.14},
			},
		},
		{
			name: "error_invalid_schema",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name:        "invalid",
						ComplexType: nil, // This should cause the decoder to fall back to YSON unmarshaling
					},
				},
			},
			nameTable: NameTable{{Name: "invalid"}},
			testData:  []byte(`"test"`),
			isErr:     false, // This should not be an error, it should fall back to YSON unmarshaling
			expected: map[string]any{
				"invalid": "test",
			},
		},
		{
			name: "struct_with_date_types",
			schema: &schema.Schema{
				Columns: []schema.Column{
					{
						Name: "event",
						ComplexType: schema.Struct{
							Members: []schema.StructMember{
								{Name: "date", Type: schema.TypeDate},
								{Name: "datetime", Type: schema.TypeDatetime},
								{Name: "timestamp", Type: schema.TypeTimestamp},
							},
						},
					},
				},
			},
			nameTable: NameTable{{Name: "event"}},
			testData:  []byte(`[12345u;1234567890u;1234567890123u]`),
			expected: map[string]any{
				"event": map[string]any{
					"date":      uint64(12345),
					"datetime":  uint64(1234567890),
					"timestamp": uint64(1234567890123),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(tc.nameTable, tc.schema)

			var result map[string]any
			err := d.UnmarshalRow(Row{NewComposite(0, tc.testData)}, &result)

			if !tc.isErr {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDecodeDecimalFromBinary(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		precision int
		scale     int
		expected  string
		expectErr bool
	}{
		{
			name:      "3.1415 with precision=5, scale=4",
			data:      []byte{0x80, 0x00, 0x7A, 0xB7},
			precision: 5,
			scale:     4,
			expected:  "3.1415",
		},
		{
			name:      "-2.7182 with precision=5, scale=4",
			data:      []byte{0x7F, 0xFF, 0x95, 0xD2},
			precision: 5,
			scale:     4,
			expected:  "-2.7182",
		},
		{
			name:      "NaN (32-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFF},
			precision: 5,
			scale:     4,
			expected:  "nan",
		},
		{
			name:      "+Inf (32-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFE},
			precision: 5,
			scale:     4,
			expected:  "+inf",
		},
		{
			name:      "-Inf (32-bit)",
			data:      []byte{0x00, 0x00, 0x00, 0x02},
			precision: 5,
			scale:     4,
			expected:  "-inf",
		},
		{
			name:      "Invalid length",
			data:      []byte{0x80, 0x00, 0x7A},
			precision: 5,
			scale:     4,
			expectErr: true,
		},
		// 64-bit tests (precision 10-18)
		{
			name:      "Large positive 64-bit with precision=15, scale=2",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B},
			precision: 15,
			scale:     2,
			expected:  "1.23",
		},
		{
			name:      "Large negative 64-bit with precision=15, scale=2",
			data:      []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x85},
			precision: 15,
			scale:     2,
			expected:  "-1.23",
		},
		{
			name:      "64-bit with high precision=18, scale=10",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			precision: 18,
			scale:     10,
			expected:  "0.0000000001",
		},
		{
			name:      "64-bit with precision=12, scale=0",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64},
			precision: 12,
			scale:     0,
			expected:  "100",
		},
		// 128-bit tests (precision 19-38) - now working with big.Int
		{
			name:      "128-bit positive with precision=25, scale=5",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B},
			precision: 25,
			scale:     5,
			expected:  "0.00123",
		},
		{
			name:      "128-bit negative with precision=25, scale=5",
			data:      []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x85},
			precision: 25,
			scale:     5,
			expected:  "-0.00123",
		},
		{
			name:      "128-bit with precision=30, scale=15",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			precision: 30,
			scale:     15,
			expected:  "0.000000000000001",
		},
		{
			name:      "128-bit with precision=38, scale=0",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64},
			precision: 38,
			scale:     0,
			expected:  "100",
		},
		// 256-bit tests (precision 39-76) - now working with big.Int
		{
			name:      "256-bit positive with precision=50, scale=10",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B},
			precision: 50,
			scale:     10,
			expected:  "0.0000000123",
		},
		{
			name:      "256-bit negative with precision=50, scale=10",
			data:      []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x85},
			precision: 50,
			scale:     10,
			expected:  "-0.0000000123",
		},
		{
			name:      "256-bit with precision=76, scale=20",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			precision: 76,
			scale:     20,
			expected:  "0.00000000000000000001",
		},
		{
			name:      "256-bit with precision=40, scale=0",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64},
			precision: 40,
			scale:     0,
			expected:  "100",
		},
		// Edge cases with large scale
		{
			name:      "32-bit with maximum scale for precision=9, scale=9",
			data:      []byte{0x80, 0x00, 0x00, 0x01},
			precision: 9,
			scale:     9,
			expected:  "0.000000001",
		},
		{
			name:      "64-bit with large scale=18, precision=18",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			precision: 18,
			scale:     18,
			expected:  "0.000000000000000001",
		},
		{
			name:      "128-bit with large scale=38, precision=38",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			precision: 38,
			scale:     38,
			expected:  "0.00000000000000000000000000000000000001",
		},
		// Special values for larger precision
		{
			name:      "NaN (64-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			precision: 15,
			scale:     2,
			expected:  "nan",
		},
		{
			name:      "+Inf (64-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE},
			precision: 15,
			scale:     2,
			expected:  "+inf",
		},
		{
			name:      "-Inf (64-bit)",
			data:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
			precision: 15,
			scale:     2,
			expected:  "-inf",
		},
		{
			name:      "NaN (128-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			precision: 25,
			scale:     5,
			expected:  "nan",
		},
		{
			name:      "+Inf (128-bit)",
			data:      []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE},
			precision: 25,
			scale:     5,
			expected:  "+inf",
		},
		{
			name:      "-Inf (128-bit)",
			data:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
			precision: 25,
			scale:     5,
			expected:  "-inf",
		},
		// Large value tests - now working with big.Int
		{
			name:      "128-bit large positive value",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			precision: 25,
			scale:     5,
			expected:  "92233720368547.75808",
		},
		{
			name:      "256-bit large positive value",
			data:      []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			precision: 50,
			scale:     10,
			expected:  "922337203.6854775808",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeDecimalFromBinary(tt.data, tt.precision, tt.scale)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDecoder_decodeReflect(t *testing.T) {
	nameTable := NameTable{
		{Name: "composite_field"},
	}

	d := NewDecoder(nameTable, &schema.Schema{Columns: []schema.Column{
		{Name: "composite_field", ComplexType: schema.Struct{
			Members: []schema.StructMember{
				{Name: "id", Type: schema.TypeInt64},
				{Name: "name", Type: schema.TypeString},
			},
		}},
	}})

	type testStruct struct {
		CompositeField any `yson:"composite_field"`
	}

	testData := []byte(`[123;"test"]`)

	var result testStruct
	err := d.UnmarshalRow(Row{NewComposite(0, testData)}, &result)

	require.NoError(t, err)
	require.NotNil(t, result.CompositeField)

	compositeMap, ok := result.CompositeField.(map[string]any)
	require.True(t, ok)
	require.Equal(t, int64(123), compositeMap["id"])
	require.Equal(t, "test", compositeMap["name"])
}

func TestDecoder_decodeReflectWithAnyPointer(t *testing.T) {
	nameTable := NameTable{
		{Name: "field"},
	}

	d := NewDecoder(nameTable, &schema.Schema{Columns: []schema.Column{
		{Name: "field", ComplexType: schema.List{
			Item: schema.TypeInt64,
		}},
	}})

	type testStruct struct {
		Field *any `yson:"field"`
	}

	testData := []byte(`[1;2;3;4;5]`)

	var result testStruct
	err := d.UnmarshalRow(Row{NewComposite(0, testData)}, &result)

	require.NoError(t, err)
	require.NotNil(t, result.Field)

	list, ok := (*result.Field).([]any)
	require.True(t, ok)
	require.Len(t, list, 5)
	require.Equal(t, int64(1), list[0])
	require.Equal(t, int64(5), list[4])
}
