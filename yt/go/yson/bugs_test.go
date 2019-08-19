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

type (
	MyInt      int
	MyUint     uint
	MyIntSlice []int
	MyString   string

	TypedefTest struct {
		I  MyInt
		U  MyUint
		II MyIntSlice
		S  MyString
	}
)

func TestTypedefTypes(t *testing.T) {
	in := TypedefTest{
		I:  1337,
		U:  1234,
		II: []int{1, 3, 3, 7},
		S:  "foobar",
	}

	var out TypedefTest

	js, err := json.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(js, &out))
	require.Equal(t, in, out)

	ys, err := yson.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, yson.Unmarshal(ys, &out))
	require.Equal(t, in, out)
}

func TestMapKeys(t *testing.T) {
	in := map[MyString]int{
		"foo": 1,
		"bar": 2,
	}
	var out map[MyString]int

	ys, err := yson.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, yson.Unmarshal(ys, &out))
	require.Equal(t, in, out)
}
