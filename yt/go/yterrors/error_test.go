package yterrors

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/xerrors"
)

func TestNewError(t *testing.T) {
	err0 := Err("internal error")

	err1 := Err(ErrorCode(100), "tablet error", ErrorAttr{"tablet_id", "foobar"})
	assert.Equal(t, "foobar", err1.(*Error).Attributes["tablet_id"])

	err2 := Err("nested error", err0, err1)

	_ = err2

	require.Panics(t, func() {
		_ = Err(struct{}{})
	})
}

func TestContainsCode(t *testing.T) {
	err0 := Err(ErrorCode(101))

	require.False(t, ContainsErrorCode(err0, 100))
	require.True(t, ContainsErrorCode(err0, 101))

	err1 := Err(err0)

	require.False(t, ContainsErrorCode(err1, 100))
	require.True(t, ContainsErrorCode(err1, 101))

	err2 := Err(ErrorCode(102), err1)

	require.False(t, ContainsErrorCode(err2, 100))
	require.True(t, ContainsErrorCode(err2, 101))
	require.True(t, ContainsErrorCode(err2, 102))

	err3 := xerrors.Errorf("HTTP error: %w", Err(ErrorCode(101)))
	require.True(t, ContainsErrorCode(err3, 101))
}

func TestErrorsInterop(t *testing.T) {
	err0 := Err(fmt.Errorf("example error"))

	require.True(t, ContainsErrorCode(err0, 1))
	require.False(t, ContainsErrorCode(fmt.Errorf("example error"), 1))
}

func TestErrorPrinting(t *testing.T) {
	for _, testCase := range []struct {
		err   error
		brief string
		full  string
	}{
		{
			err:   Err("Tablet not found"),
			brief: "tablet not found",
			full: `
tablet not found`,
		},
		{
			err:   Err(ErrorCode(500), "Tablet not found"),
			brief: "tablet not found",
			full: `
tablet not found:
      code: 500`,
		},
		{
			err:   Err("Tablet not found", Err("Cypress error")),
			brief: "tablet not found: cypress error",
			full: `
tablet not found:
    cypress error`,
		},
		{
			err:   Err("Tablet not found", Err("Cypress error", Err("Rpc error"))),
			brief: "tablet not found: cypress error: rpc error",
			full: `
tablet not found:
    cypress error
    rpc error`,
		},
		{
			err:   Err("Tablet not found", Err("Retry error", ErrorAttr{"foo", "bar"}), Err("Cypress error", ErrorAttr{"zog", "zog"})),
			brief: "tablet not found: cypress error",
			full: `
tablet not found:
    retry error
      foo: bar
    cypress error
      zog: zog`,
		},
		{
			err:   Err("Retry error", ErrorAttr{"foo", "bar"}, ErrorAttr{"a", "b"}),
			brief: "retry error",
			full: `
retry error:
      a:   b
      foo: bar`,
		},
	} {
		assert.Equal(t, testCase.brief, fmt.Sprintf("%v", testCase.err))
		assert.Equal(t, testCase.full[1:], fmt.Sprintf("%+v", testCase.err))
	}
}

func TestJSONInt64Attr(t *testing.T) {
	ytErr := Err("error", Attr("i", uint64(math.MaxUint64)))

	js, err := json.Marshal(ytErr)
	require.NoError(t, err)

	var out Error
	require.NoError(t, json.Unmarshal(js, &out))

	// Information is lost after JSON conversion.
	require.NotEqual(t, fmt.Sprintf("%+v", ytErr), fmt.Sprintf("%+v", &out))
}

func TestJSON(t *testing.T) {
	ytErr := Err("моя папка/таблица", Attr("s", "Мир"))

	js, err := json.Marshal(ytErr)
	require.NoError(t, err)

	var out Error
	require.NoError(t, json.Unmarshal(js, &out))

	require.Equal(t, ytErr.(*Error).Message, out.Message)
	require.Equal(t, ytErr.(*Error).Attributes, out.Attributes)
}
