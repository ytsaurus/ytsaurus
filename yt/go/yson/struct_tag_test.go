package yson

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStructTagParse(t *testing.T) {
	for _, testCase := range []struct {
		fieldName string
		tagValue  reflect.StructTag
		tag       *Tag
		skip      bool
	}{
		{
			"MyField",
			"",
			&Tag{Name: "MyField"},
			false,
		},
		{
			"MySkippedField",
			`yson:"-"`,
			nil,
			true,
		},
		{
			"MyValue",
			`yson:",value"`,
			&Tag{Value: true},
			false,
		},
		{
			"MyKey",
			`yson:",omitempty,key"`,
			&Tag{Name: "MyKey", Omitempty: true, Key: true},
			false,
		},
		{
			"MyAttr",
			`yson:"format,attr"`,
			&Tag{Name: "format", Attr: true},
			false,
		},
	} {
		t.Run(testCase.fieldName, func(t *testing.T) {
			tag, skip := ParseTag(testCase.fieldName, testCase.tagValue)
			require.Equal(t, testCase.skip, skip)
			require.Equal(t, testCase.tag, tag)
		})
	}
}
