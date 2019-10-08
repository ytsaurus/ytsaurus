package tests_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yp/go/yson/internal/proto-yson-gen/tests"
	"a.yandex-team.ru/yp/go/yson/ytypes"
	"a.yandex-team.ru/yt/go/yson"
)

func TestEnum(t *testing.T) {
	t.Run("unknown_value", func(t *testing.T) {
		var obj tests.Enum

		err := yson.Unmarshal([]byte(`unknown_value`), &obj)
		require.Error(t, err)
	})

	t.Run("encode", func(t *testing.T) {
		obj := tests.Enum_Foo

		ysonMarshaled, err := yson.Marshal(obj)
		require.NoError(t, err)
		require.Equal(t, []byte(`foo`), ysonMarshaled)

		jsonMarshaled, err := json.Marshal(obj)
		require.NoError(t, err)
		require.Equal(t, []byte(`"foo"`), jsonMarshaled)
	})

	t.Run("decode", func(t *testing.T) {
		var (
			jsonObj     tests.Enum
			ysonObj     tests.Enum
			expectedObj = tests.Enum_Bar
		)

		err := yson.Unmarshal([]byte(`bar`), &ysonObj)
		require.NoError(t, err)
		require.Equal(t, expectedObj, ysonObj)

		err = json.Unmarshal([]byte(`"bar"`), &jsonObj)
		require.NoError(t, err)
		require.Equal(t, expectedObj, jsonObj)
	})
}

func TestSimple(t *testing.T) {
	t.Run("encode", func(t *testing.T) {
		obj := tests.Simple{
			Enum: tests.Enum_Bar.Enum(),
			Val:  ptr.String("test"),
			Nested: &tests.Simple_Nested{
				NestedVal: ptr.String("nested"),
			},
			Timestamp: ytypes.TimestampYson(time.Unix(1000, 1).UTC()),
		}

		ysonMarshaled, err := yson.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{enum=bar;val=test;nested={"nested_val"=nested;};timestamp={seconds=1000;nanos=1;};}`),
			ysonMarshaled,
		)

		jsonMarshaled, err := json.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{"enum":"bar","val":"test","nested":{"nested_val":"nested"},"timestamp":{"seconds":1000,"nanos":1}}`),
			jsonMarshaled,
		)
	})

	t.Run("decode", func(t *testing.T) {
		var (
			jsonObj     tests.Simple
			ysonObj     tests.Simple
			expectedObj = tests.Simple{
				Enum: tests.Enum_Bar.Enum(),
				Val:  ptr.String("test"),
				Nested: &tests.Simple_Nested{
					NestedVal: ptr.String("nested"),
				},
				Timestamp: ytypes.TimestampYson(time.Unix(1000, 1).UTC()),
			}
		)

		err := yson.Unmarshal(
			[]byte(`{enum=bar;val=test;nested={"nested_val"=nested;};timestamp={seconds=1000;nanos=1;};}`),
			&ysonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, ysonObj)

		err = json.Unmarshal(
			[]byte(`{"enum":"bar","val":"test","nested":{"nested_val":"nested"},"timestamp":{"seconds":1000,"nanos":1}}`),
			&jsonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, jsonObj)
	})
}

func TestWithDefaults(t *testing.T) {
	t.Run("encode_with", func(t *testing.T) {
		obj := tests.WithDefaults{
			Enum: tests.Enum_Foo.Enum(),
			Val:  ptr.String("test"),
		}

		ysonMarshaled, err := yson.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{enum=foo;val=test;}`),
			ysonMarshaled,
		)

		jsonMarshaled, err := json.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{"enum":"foo","val":"test"}`),
			jsonMarshaled,
		)
	})

	t.Run("decode_with", func(t *testing.T) {
		var (
			jsonObj     tests.WithDefaults
			ysonObj     tests.WithDefaults
			expectedObj = tests.WithDefaults{
				Enum: tests.Enum_Foo.Enum(),
				Val:  ptr.String("test"),
			}
		)

		err := yson.Unmarshal(
			[]byte(`{enum=foo;val=test;}`),
			&ysonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, ysonObj)
		require.Equal(t, "test", ysonObj.GetVal())

		err = json.Unmarshal(
			[]byte(`{"enum":"foo","val":"test"}`),
			&jsonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, jsonObj)
		require.Equal(t, "test", jsonObj.GetVal())
	})

	t.Run("encode_without", func(t *testing.T) {
		obj := tests.WithDefaults{
			Enum: tests.Enum_Foo.Enum(),
		}

		ysonMarshaled, err := yson.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{enum=foo;}`),
			ysonMarshaled,
		)

		jsonMarshaled, err := json.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{"enum":"foo"}`),
			jsonMarshaled,
		)
	})

	t.Run("decode_without", func(t *testing.T) {
		var (
			jsonObj     tests.WithDefaults
			ysonObj     tests.WithDefaults
			expectedObj = tests.WithDefaults{
				Enum: tests.Enum_Foo.Enum(),
			}
		)

		err := yson.Unmarshal(
			[]byte(`{enum=foo;}`),
			&ysonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, ysonObj)
		require.Equal(t, "some_val", ysonObj.GetVal())

		err = json.Unmarshal(
			[]byte(`{"enum":"foo"}`),
			&jsonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, jsonObj)
		require.Equal(t, "some_val", jsonObj.GetVal())
	})

	t.Run("encode_empty", func(t *testing.T) {
		obj := tests.WithDefaults{
			Enum: tests.Enum_Foo.Enum(),
			Val:  ptr.String(""),
		}

		ysonMarshaled, err := yson.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{enum=foo;val="";}`),
			ysonMarshaled,
		)

		jsonMarshaled, err := json.Marshal(obj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{"enum":"foo","val":""}`),
			jsonMarshaled,
		)
	})

	t.Run("decode_empty", func(t *testing.T) {
		var (
			jsonObj     tests.WithDefaults
			ysonObj     tests.WithDefaults
			expectedObj = tests.WithDefaults{
				Enum: tests.Enum_Foo.Enum(),
				Val:  ptr.String(""),
			}
		)

		err := yson.Unmarshal(
			[]byte(`{enum=foo;val="";}`),
			&ysonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, ysonObj)
		require.Equal(t, "", ysonObj.GetVal())

		err = json.Unmarshal(
			[]byte(`{"enum":"foo","val":""}`),
			&jsonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, jsonObj)
		require.Equal(t, "", jsonObj.GetVal())
	})
}

func TestWithRequired(t *testing.T) {
	t.Run("encode", func(t *testing.T) {
		okObj := tests.WithRequired{
			Val: ptr.String("test"),
		}

		failObj := tests.WithRequired{
			Enum: tests.Enum_Foo.Enum(),
		}

		ysonMarshaled, err := yson.Marshal(okObj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{val=test;}`),
			ysonMarshaled,
		)

		_, err = yson.Marshal(failObj)
		require.Error(t, err)

		jsonMarshaled, err := json.Marshal(okObj)
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte(`{"val":"test"}`),
			jsonMarshaled,
		)

		_, err = json.Marshal(failObj)
		require.Error(t, err)
	})

	t.Run("decode", func(t *testing.T) {
		var (
			jsonObj     tests.WithRequired
			ysonObj     tests.WithRequired
			expectedObj = tests.WithRequired{
				Enum: tests.Enum_Foo.Enum(),
				Val:  ptr.String("test"),
			}
		)

		err := yson.Unmarshal(
			[]byte(`{enum=foo;val=test;}`),
			&ysonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, ysonObj)

		err = yson.Unmarshal(
			[]byte(`{enum=foo;}`),
			&ysonObj,
		)
		require.Error(t, err)

		err = json.Unmarshal(
			[]byte(`{"enum":"foo","val":"test"}`),
			&jsonObj,
		)
		require.NoError(t, err)
		require.EqualValues(t, expectedObj, jsonObj)

		err = json.Unmarshal(
			[]byte(`{"enum":"foo"}`),
			&jsonObj,
		)
		require.Error(t, err)
	})
}

func TestOneOf(t *testing.T) {
	cases := map[string]struct {
		obj    tests.OneOf
		action interface{}
		yson   string
		json   string
	}{
		"complex": {
			obj: tests.OneOf{
				Complex: &tests.OneOf_ComplexAction{
					Url: ptr.String("test_url"),
				},
			},
			action: &tests.OneOf_Complex{
				Value: &tests.OneOf_ComplexAction{
					Url: ptr.String("test_url"),
				},
			},
			yson: `{complex={url="test_url";};}`,
			json: `{"complex":{"url":"test_url"}}`,
		},
		"simple": {
			obj: tests.OneOf{
				Simple: ptr.String("simple"),
			},
			action: &tests.OneOf_Simple{
				Value: ptr.String("simple"),
			},
			yson: `{simple=simple;}`,
			json: `{"simple":"simple"}`,
		},
		"empty": {
			obj:    tests.OneOf{},
			action: nil,
			yson:   `{}`,
			json:   `{}`,
		},
	}

	for name, tc := range cases {
		t.Run(name+"_yson", func(t *testing.T) {
			encoded, err := yson.Marshal(tc.obj)
			require.NoError(t, err)
			require.Equal(t, []byte(tc.yson), encoded)

			var decoded tests.OneOf
			err = yson.Unmarshal(encoded, &decoded)
			require.NoError(t, err)
			require.EqualValues(t, tc.obj, decoded)
			require.EqualValues(t, tc.action, decoded.GetAction())
		})

		t.Run(name+"_json", func(t *testing.T) {
			encoded, err := json.Marshal(tc.obj)
			require.NoError(t, err)
			require.Equal(t, []byte(tc.json), encoded)

			var decoded tests.OneOf
			err = json.Unmarshal(encoded, &decoded)
			require.NoError(t, err)
			require.EqualValues(t, tc.obj, decoded)
			require.EqualValues(t, tc.action, decoded.GetAction())
		})
	}

}
