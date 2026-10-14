package skiff

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
)

type TestRow struct {
	First  *int64 `yson:"first"`
	Second int64  `yson:"second,omitempty"`
	Third  string `yson:"third"`
}

var (
	testSchema = Schema{
		Type: TypeTuple,
		Children: []Schema{
			systemColumns["$key_switch"],
			systemColumns["$row_index"],
			systemColumns["$range_index"],
			optionalColumn("first", TypeInt64),
			optionalColumn("second", TypeInt64),
			{Type: TypeString32, Name: "third"},
		},
	}

	testFormat = Format{
		Name:           "skiff",
		TableSchemas:   []any{&testSchema},
		SchemaRegistry: nil,
	}
)

func TestDecoder(t *testing.T) {
	input := bytes.NewBuffer([]byte{
		0x00, 0x00, // first row
		0x01,                                                 // key switch
		0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // row index == 2
		0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // range index == 3
		0x00,                                                 // first field is missing
		0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // second field is present
		0x08, 0x00, 0x00, 0x00, 'a', 'b', 'b', 'a', 'c', 'a', 'b', 'a', // third field is string

		0x00, 0x00, // second row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x01, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first field is present
		0x00,                   // second field is missing
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // third row
		0x00,                   // no key switch
		0x00,                   // no row index
		0x00,                   // no range index
		0x00,                   // first field is missing
		0x00,                   // second field is missing
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // forth row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x01, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first field is present
		0x00,                                                 // second field is missing
		0x06, 0x00, 0x00, 0x00, 'f', 'o', 'o', 'b', 'a', 'r', // third field is string

	})

	decoder, err := NewDecoder(input, testFormat)
	require.NoError(t, err)

	var row TestRow

	require.True(t, decoder.Next())
	tableIndex := decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch := decoder.KeySwitch()
	require.True(t, keySwitch)
	rowIndex := decoder.RowIndex()
	require.Equal(t, int64(2), rowIndex)
	rangeIndex := decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)
	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{Second: 3, Third: "abbacaba"})

	require.True(t, decoder.Next())
	tableIndex = decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch = decoder.KeySwitch()
	require.False(t, keySwitch)
	rowIndex = decoder.RowIndex()
	require.Equal(t, int64(3), rowIndex)
	rangeIndex = decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)

	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{First: ptr.Int64(5)})

	// Test backup() && checkpoint().
	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{First: ptr.Int64(5)})

	// Row is skipped.
	require.True(t, decoder.Next())
	tableIndex = decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch = decoder.KeySwitch()
	require.False(t, keySwitch)
	rowIndex = decoder.RowIndex()
	require.Equal(t, int64(4), rowIndex)
	rangeIndex = decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)

	require.True(t, decoder.Next())

	expected := map[string]any{
		"first":  int64(6),
		"second": nil,
		"third":  "foobar",
	}

	var i any
	require.NoError(t, decoder.Scan(&i))
	require.Equal(t, expected, i)

	var m map[string]any
	require.NoError(t, decoder.Scan(&m))
	require.Equal(t, expected, m)

	var intMap map[string]int
	require.Error(t, decoder.Scan(&intMap))

	require.False(t, decoder.Next())
	require.NoError(t, decoder.Err())
}

func TestDecoderScanRawRow(t *testing.T) {
	rowSchema := Schema{Type: TypeTuple, Children: []Schema{
		{Name: "id", Type: TypeInt64},
		optionalColumn("name", TypeString32),
		{Name: "flags", Type: TypeRepeatedVariant8, Children: []Schema{{Type: TypeBoolean}}},
		{Name: "point", Type: TypeTuple, Children: []Schema{{Type: TypeInt16}, {Type: TypeUint32}}},
		{Name: "choice", Type: TypeVariant16, Children: []Schema{{Type: TypeDouble}, {Type: TypeYSON32}}},
		{Name: "narrow_decimal", Type: TypeInt128},
		{Name: "wide_decimal", Type: TypeInt256},
		{Name: "codes", Type: TypeRepeatedVariant16, Children: []Schema{{Type: TypeUint8}}},
	}}
	first := bytes.Join([][]byte{
		{0x01, 0, 0, 0, 0, 0, 0, 0},
		{0x01, 0x02, 0, 0, 0, 'a', 'b'},
		{0x00, 0x01, 0x00, 0x00, 0xff},
		{0xff, 0xff, 0x05, 0, 0, 0},
		{0x01, 0x00, 0x01, 0, 0, 0, '#'},
		bytes.Repeat([]byte{0x11}, 16),
		bytes.Repeat([]byte{0x22}, 32),
		{0x00, 0x00, 0x07, 0xff, 0xff},
	}, nil)
	second := bytes.Join([][]byte{
		{0x02, 0, 0, 0, 0, 0, 0, 0},
		{0x00},
		{0xff},
		{0, 0, 0, 0, 0, 0},
		{0x00, 0x00, 0, 0, 0, 0, 0, 0, 0xe0, 0x3f},
		make([]byte, 16),
		make([]byte, 32),
		{0xff, 0xff},
	}, nil)
	input := bytes.Join([][]byte{{0, 0}, first, {0, 0}, second}, nil)

	decoder, err := NewDecoder(bytes.NewBuffer(input), Format{Name: "skiff", TableSchemas: []any{&rowSchema}})
	require.NoError(t, err)

	var rows []RawRow
	for decoder.Next() {
		var row RawRow
		require.NoError(t, decoder.Scan(&row))
		rows = append(rows, row)
	}
	require.NoError(t, decoder.Err())
	require.Equal(t, []RawRow{first, second}, rows)

	decoder, err = NewDecoder(bytes.NewBuffer(input), Format{Name: "skiff", TableSchemas: []any{&rowSchema}})
	require.NoError(t, err)
	require.True(t, decoder.Next())
	var row RawRow
	require.NoError(t, decoder.Scan(&row))
	row[0] ^= 0xff
	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, RawRow(first), row)

	decoder, err = NewDecoder(bytes.NewBuffer([]byte{
		0x00, 0x00, 0x01, 0x01, 0x02, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x03, 0, 0, 0, 0, 0, 0, 0,
		0x00, 0x01, 0x03, 0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0, 'a',
	}), testFormat)
	require.NoError(t, err)
	require.True(t, decoder.Next())
	var withoutSystemColumns RawRow
	require.NoError(t, decoder.Scan(&withoutSystemColumns))
	require.Equal(t, RawRow{0x00, 0x01, 0x03, 0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0, 'a'}, withoutSystemColumns)
}

func TestDecoderScanRawRowErrors(t *testing.T) {
	rowSchema := Schema{Type: TypeTuple, Children: []Schema{optionalColumn("name", TypeString32)}}

	for _, tc := range []struct {
		name     string
		input    []byte
		errorMsg string
	}{
		{name: "truncated row", input: []byte{0, 0, 0x01, 0x02, 0, 0, 0, 'a'}, errorMsg: "unexpected EOF"},
		{name: "row without fields", input: []byte{0, 0}, errorMsg: "unexpected EOF"},
		{name: "unknown variant tag", input: []byte{0, 0, 0x05}, errorMsg: "out of range"},
		{name: "oversized string", input: []byte{0, 0, 0x01, 0xff, 0xff, 0xff, 0xff}, errorMsg: "too large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewDecoder(bytes.NewBuffer(tc.input), Format{Name: "skiff", TableSchemas: []any{&rowSchema}})
			require.NoError(t, err)

			require.True(t, decoder.Next())
			var row RawRow
			require.ErrorContains(t, decoder.Scan(&row), tc.errorMsg)
			require.False(t, decoder.Next())
			require.ErrorContains(t, decoder.Err(), tc.errorMsg)
		})
	}
}

func TestDecodeGenericNestedOptionalStruct(t *testing.T) {
	// Column "s": struct<inner: optional<struct<a: int64; b: string>>>.
	innerStruct := schema.Struct{Members: []schema.StructMember{
		{Name: "a", Type: schema.TypeInt64},
		{Name: "b", Type: schema.TypeString},
	}}
	tableSchema := &schema.Schema{Columns: []schema.Column{{
		Name: "s",
		ComplexType: schema.Struct{Members: []schema.StructMember{
			{Name: "inner", Type: schema.Optional{Item: innerStruct}},
		}},
	}}}
	skiffSchema := FromTableSchema(*tableSchema)
	format := Format{Name: "skiff", TableSchemas: []any{&skiffSchema}}

	input := bytes.NewBuffer([]byte{
		0x00, 0x00, // first row: table index 0
		0x00, // inner optional tag == 0 (null) — previously panicked

		0x00, 0x00, // second row: table index 0
		0x01,                                           // inner optional tag == 1 (present)
		0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // a == int64(42)
		0x02, 0x00, 0x00, 0x00, 'h', 'i', // b == "hi"
	})

	decoder, err := NewDecoder(input, format, WithDecoderTableSchemas(tableSchema))
	require.NoError(t, err)

	require.True(t, decoder.Next())
	var nullRow map[string]any
	require.NoError(t, decoder.Scan(&nullRow))
	require.Equal(t, map[string]any{"s": map[string]any{"inner": nil}}, nullRow)

	require.True(t, decoder.Next())
	var presentRow map[string]any
	require.NoError(t, decoder.Scan(&presentRow))
	require.Equal(t, map[string]any{
		"s": map[string]any{"inner": map[string]any{"a": int64(42), "b": "hi"}},
	}, presentRow)

	require.False(t, decoder.Next())
	require.NoError(t, decoder.Err())
}
