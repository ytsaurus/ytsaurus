package yterrors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

func TestNewError(t *testing.T) {
	err0 := Err("Internal error")

	err1 := Err(ErrorCode(100), "Tablet error", ErrorAttr{"tablet_id", "foobar"})
	assert.Equal(t, "foobar", err1.(*Error).Attributes["tablet_id"])

	err2 := Err("Nested error", err0, err1)

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
			brief: "yt: Tablet not found",
			full: `
yt: Tablet not found`,
		},
		{
			err:   Err(ErrorCode(500), "Tablet not found"),
			brief: "yt: Tablet not found",
			full: `
yt: Tablet not found:
      code: 500`,
		},
		{
			err:   Err("Tablet not found", Err("Cypress error")),
			brief: "yt: Tablet not found: yt: Cypress error",
			full: `
yt: Tablet not found:
    Cypress error`,
		},
		{
			err:   Err("Tablet not found", Err("Cypress error", Err("Rpc error"))),
			brief: "yt: Tablet not found: yt: Cypress error: yt: Rpc error",
			full: `
yt: Tablet not found:
    Cypress error
    Rpc error`,
		},
		{
			err:   Err("Tablet not found", Err("Retry error", ErrorAttr{"foo", "bar"}), Err("Cypress error", ErrorAttr{"zog", "zog"})),
			brief: "yt: Tablet not found: yt: Cypress error",
			full: `
yt: Tablet not found:
    Retry error
      foo: bar
    Cypress error
      zog: zog`,
		},
		{
			err:   Err("Retry error", ErrorAttr{"foo", "bar"}, ErrorAttr{"a", "b"}),
			brief: "yt: Retry error",
			full: `
yt: Retry error:
      a:   b
      foo: bar`,
		},
	} {
		assert.Equal(t, testCase.brief, fmt.Sprintf("%v", testCase.err))
		assert.Equal(t, testCase.full[1:], fmt.Sprintf("%+v", testCase.err))
	}
}
