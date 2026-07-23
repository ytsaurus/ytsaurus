//go:build cgo
// +build cgo

package gotest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/yandex/tvm/tvmauth"
)

func TestGenerateKeyPair_Simple(t *testing.T) {
	keyPair := tvmauth.GenerateKeyPair(2048)
	require.NotNil(t, keyPair)
	require.NotEmpty(t, keyPair.Private)
	require.NotEmpty(t, keyPair.Public)
}

func TestGenerateKeyPair_DifferentValues(t *testing.T) {
	keyPair := tvmauth.GenerateKeyPair(2048)
	require.NotNil(t, keyPair)
	require.NotEmpty(t, keyPair.Private)
	require.NotEmpty(t, keyPair.Public)

	anotherKeyPair := tvmauth.GenerateKeyPair(2048)
	require.NotNil(t, anotherKeyPair)
	require.NotEmpty(t, anotherKeyPair.Private)
	require.NotEmpty(t, anotherKeyPair.Public)

	require.NotEqual(t, keyPair.Private, anotherKeyPair.Private)
}
