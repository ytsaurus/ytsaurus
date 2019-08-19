package yson_test

import (
	"encoding/json"
	"testing"

	"a.yandex-team.ru/yt/go/yson"
	"github.com/stretchr/testify/require"
)

type (
	NullTestVal struct {
		Val int `json:"val,omitempty" yson:"val,omitempty"`
	}

	NullTest struct {
		Val   *NullTestVal      `json:"val,omitempty" yson:"val,omitempty"`
		Int   *int              `json:"int,omitempty" yson:"int,omitempty"`
		Map   map[string]string `json:"map,omitempty" yson:"map,omitempty"`
		Array []string          `json:"array,omitempty" yson:"array,omitempty"`
	}
)

func TestNullHandling(t *testing.T) {
	var jsonTest NullTest
	msgJson := `{"int": null, "map": null, "array": null}`
	require.NoError(t, json.Unmarshal([]byte(msgJson), &jsonTest))

	var ysonTest NullTest
	msgYson := `{int=#;map=#;array=#}`
	require.NoError(t, yson.Unmarshal([]byte(msgYson), &ysonTest))

	require.Equal(t, jsonTest, ysonTest)
}
