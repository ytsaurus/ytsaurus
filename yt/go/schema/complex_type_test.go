package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
)

func TestComplexType(t *testing.T) {
	opt := Optional{Item: TypeInt64}

	ys, err := yson.MarshalFormat(opt, yson.FormatText)
	require.NoError(t, err)
	require.Equal(t, `{"type_name"=optional;item=int64;}`, string(ys))

	tuple := Tuple{
		Elements: []TupleElement{
			{Type: TypeString},
			{Type: TypeFloat64},
		},
	}

	variant := Variant{
		Elements: []TupleElement{
			{Type: TypeFloat64},
			{Type: TypeString},
		},
	}

	namedVariant := Variant{
		Members: []StructMember{
			{Name: "f", Type: TypeFloat64},
			{Name: "s", Type: TypeString},
		},
	}

	dict := Dict{
		Key:   TypeString,
		Value: TypeInt64,
	}

	s := Struct{
		Members: []StructMember{
			{
				Name: "string",
				Type: TypeString,
			},
			{
				Name: "decimal",
				Type: Decimal{Precision: 10, Scale: 3},
			},
			{
				Name: "optional",
				Type: Optional{Item: TypeBoolean},
			},
			{
				Name: "list",
				Type: List{Item: TypeInt64},
			},
			{
				Name: "tuple",
				Type: tuple,
			},
			{
				Name: "variant",
				Type: variant,
			},
			{
				Name: "named_variant",
				Type: namedVariant,
			},
			{
				Name: "dict",
				Type: dict,
			},
			{
				Name: "tagged",
				Type: Tagged{Tag: "test", Item: TypeInt64},
			},
		},
	}

	ys, err = yson.Marshal(s)
	require.NoError(t, err)

	var ct ComplexType
	require.NoError(t, yson.Unmarshal(ys, &ct))
	require.Equal(t, ct, s)
}
