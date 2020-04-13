package yterrors

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNilError(t *testing.T) {
	require.NoError(t, FromError(nil))
}

func TestIsWrapping(t *testing.T) {
	unrelatedErr := errors.New("unrelated")
	innerErr := errors.New("inner")
	err := Err("operation failed", innerErr)

	require.True(t, errors.Is(err, innerErr))
	require.False(t, errors.Is(err, unrelatedErr))
}

func TestAsWrapping(t *testing.T) {
	innerErr := &os.PathError{Path: "//home", Err: errors.New("open failed")}
	err := Err("operation failed", innerErr)

	var pathErr *os.PathError
	require.True(t, errors.As(err, &pathErr))
	require.Equal(t, innerErr, pathErr)

	var typeErr *json.UnsupportedTypeError
	require.False(t, errors.As(err, &typeErr))
}

func TestErrorChainConversion(t *testing.T) {
	innerErr := errors.New("inner")
	middleErr := fmt.Errorf("middle: %w", innerErr)
	outerErr := fmt.Errorf("outer: %w", middleErr)

	yt := FromError(outerErr).(*Error)
	assert.Equal(t, yt.Error(), outerErr.Error())

	expected := &Error{
		origError: outerErr,

		Code:    1,
		Message: "Outer",
		InnerErrors: []*Error{
			{
				origError: middleErr,

				Code:    1,
				Message: "Middle",
				InnerErrors: []*Error{
					{
						origError: innerErr,

						Code:    1,
						Message: "Inner",
					},
				},
			},
		},
	}

	// spew.Config.ContinueOnMethod = true
	// spew.Fdump(os.Stderr, yt)
	// spew.Fdump(os.Stderr, expected)

	require.Equal(t, yt, expected)
}

func TestYTErrorInsideGoError(t *testing.T) {
	ytErr := Err("Request limit exceeded").(*Error)
	goErr := fmt.Errorf("write failed: %w", ytErr)

	converted := FromError(goErr).(*Error)
	expected := &Error{
		origError: goErr,
		Code:      1,
		Message:   "Write failed",

		InnerErrors: []*Error{ytErr},
	}

	assert.Equal(t, expected, converted)
	assert.Equal(t, "write failed: request limit exceeded", goErr.Error())
}

func TestCaseConversions(t *testing.T) {
	for _, testCase := range []struct {
		in, lower, upper string
	}{
		{
			in:    "write error",
			upper: "Write error",
		},
		{
			in:    "error",
			upper: "Error",
		},
		{
			in:    "Write error",
			lower: "write error",
		},
		{
			in:    "write error",
			upper: "Write error",
		},
		{
			in:    "Write error; Permission denied",
			lower: "write error; permission denied",
		},
		{
			in:    "Write error; YT permission denied",
			lower: "write error; YT permission denied",
		},
		{
			in:    "write error: permission denied",
			upper: "Write error: Permission denied",
		},
		{
			in:    "idm: request failed",
			upper: "idm: Request failed",
		},
		{
			in:    "yson: type error",
			upper: "yson: Type error",
		},
		{
			in:    "EOF",
			lower: "EOF",
		},
	} {
		if testCase.lower != "" {
			assert.Equal(t, testCase.lower, uncapitalize(testCase.in))
		}

		if testCase.upper != "" {
			assert.Equal(t, testCase.upper, capitalize(testCase.in))
		}
	}
}
