package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type innerStruct struct {
	Field int
}

type testBasicTypes struct {
	I   int
	I64 int64
	I32 int32
	I16 int16

	U   uint
	U64 uint64
	U32 uint32
	U16 uint16

	S string
	B []byte

	A0 interface{}
	A1 innerStruct
	A2 map[string]interface{}
}

func TestInfer(t *testing.T) {
	s, err := Infer(&testBasicTypes{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "I", Type: TypeInt64, Required: true},
			{Name: "I64", Type: TypeInt64, Required: true},
			{Name: "I32", Type: TypeInt32, Required: true},
			{Name: "I16", Type: TypeInt16, Required: true},
			{Name: "U", Type: TypeUint64, Required: true},
			{Name: "U64", Type: TypeUint64, Required: true},
			{Name: "U32", Type: TypeUint32, Required: true},
			{Name: "U16", Type: TypeUint16, Required: true},
			{Name: "S", Type: TypeString, Required: true},
			{Name: "B", Type: TypeBytes, Required: true},
			{Name: "A0", Type: TypeAny},
			{Name: "A1", Type: TypeAny},
			{Name: "A2", Type: TypeAny},
		},
	})
}

type textMarhaler struct{}

func (*textMarhaler) MarshalText() (text []byte, err error) {
	panic("implement me")
}

type valueTextMarhaler struct{}

func (valueTextMarhaler) MarshalText() (text []byte, err error) {
	panic("implement me")
}

type binaryMarhaler struct{}

func (*binaryMarhaler) MarshalBinary() (text []byte, err error) {
	panic("implement me")
}

type valueBinaryMarhaler struct{}

func (valueBinaryMarhaler) MarshalBinary() (text []byte, err error) {
	panic("implement me")
}

type testMarshalers struct {
	M0 textMarhaler
	M1 valueTextMarhaler
	M2 binaryMarhaler
	M3 valueBinaryMarhaler

	O0 *textMarhaler
	O1 *valueTextMarhaler
	O2 *binaryMarhaler
	O3 *valueBinaryMarhaler
}

func TestInferMarshalerTypes(t *testing.T) {
	v := &testMarshalers{}

	s, err := Infer(v)
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "M0", Type: TypeString, Required: true},
			{Name: "M1", Type: TypeString, Required: true},
			{Name: "M2", Type: TypeBytes, Required: true},
			{Name: "M3", Type: TypeBytes, Required: true},

			{Name: "O0", Type: TypeString},
			{Name: "O1", Type: TypeString},
			{Name: "O2", Type: TypeBytes},
			{Name: "O3", Type: TypeBytes},
		},
	})
}
