package datadir

import (
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"a.yandex-team.ru/library/go/test/yatest"
)

func TestDataDir(t *testing.T) {
	checkHash := func(path string, hash string) {
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		h := sha3.New224()
		_, err = io.Copy(h, f)
		require.NoError(t, err)

		assert.Equal(t, hash, hex.EncodeToString(h.Sum(nil)))
	}

	checkHash("layers/static-bin.tar.gz", "d0f3dca5a1f47cd40ad5ae861cafceeb21666ed5afe3d801ace01545")
	checkHash("layers/static_cat", "693c888fd7d34f65c4a80b058f7fd87c62809e2629aaf29b44f8dd6b")
}

func TestDuplicateFiles(t *testing.T) {
	_, err := os.Stat(yatest.SourcePath("yt/go/ytrecipe/test/files/a.txt"))
	require.NoError(t, err)

	_, err = os.Stat(yatest.SourcePath("yt/go/ytrecipe/test/files/b.txt"))
	require.NoError(t, err)

	_, err = os.Stat(yatest.SourcePath("yt/go/ytrecipe/test/files/aba"))
	require.NoError(t, err)

	_, err = os.Stat(yatest.SourcePath("yt/go/ytrecipe/test/files/abacaba"))
	require.NoError(t, err)
}
