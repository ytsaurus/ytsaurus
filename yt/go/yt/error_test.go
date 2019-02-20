package yt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

func TestNewError(t *testing.T) {
	err0 := Err("Internal error")

	err1 := Err(ErrorCode(100), "Tablet error", ErrorAttr{"tablet_id", "foobar"})

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
