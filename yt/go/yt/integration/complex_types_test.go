package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestComplexType(t *testing.T) {
	t.Parallel()
	env := yttest.New(t)

	tuple := schema.Tuple{
		Elements: []schema.TupleElement{
			{Type: schema.TypeString},
			{Type: schema.TypeFloat64},
		},
	}

	variant := schema.Variant{
		Elements: []schema.TupleElement{
			{Type: schema.TypeFloat64},
			{Type: schema.TypeString},
		},
	}

	namedVariant := schema.Variant{
		Members: []schema.StructMember{
			{Name: "f", Type: schema.TypeFloat64},
			{Name: "s", Type: schema.TypeString},
		},
	}

	dict := schema.Dict{
		Key:   schema.TypeString,
		Value: schema.TypeInt64,
	}

	s := schema.Struct{
		Members: []schema.StructMember{
			{
				Name: "string",
				Type: schema.TypeString,
			},
			{
				Name: "decimal",
				Type: schema.Decimal{Precision: 10, Scale: 3},
			},
			{
				Name: "optional",
				Type: schema.Optional{Item: schema.TypeBoolean},
			},
			{
				Name: "list",
				Type: schema.List{Item: schema.TypeInt64},
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
				Type: schema.Tagged{Tag: "test", Item: schema.TypeInt64},
			},
			{
				Name: "any",
				Type: schema.Optional{Item: schema.TypeAny},
			},
		},
	}

	testSchema := schema.Schema{
		Columns: []schema.Column{
			{
				Name:        "c0",
				ComplexType: s,
			},
			{
				Name:        "b0",
				ComplexType: schema.TypeBoolean,
			},
		},
	}

	name := env.TmpPath()
	_, err := env.YT.CreateNode(env.Ctx, name, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema": testSchema,
		},
	})
	require.NoError(t, err)

	var readSchema schema.Schema
	require.NoError(t, env.YT.GetNode(env.Ctx, name.Attr("schema"), &readSchema, nil))

	readSchema.Columns[0].Type = ""
	readSchema.Columns[0].Required = false
	readSchema.Columns[1].Type = ""
	readSchema.Columns[1].Required = false

	if !assert.True(t, readSchema.Equal(testSchema)) {
		require.Equal(t, readSchema, testSchema)
	}
}
