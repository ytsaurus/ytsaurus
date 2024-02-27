package integration

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/blockcodecs"
	_ "go.ytsaurus.tech/library/go/blockcodecs/all"
)

func TestDecoder_CheckEOF(t *testing.T) {
	var buf bytes.Buffer

	data := []byte("hello")

	w := blockcodecs.NewEncoder(&buf, blockcodecs.FindCodecByName("null"))
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	checkOK := func(r io.Reader) {
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, buf, data)
	}

	d := blockcodecs.NewDecoder(bytes.NewBuffer(buf.Bytes()))
	checkOK(d)

	withTail := bytes.NewBuffer(buf.Bytes())
	withTail.WriteByte('X')

	d = blockcodecs.NewDecoder(withTail)
	checkOK(d)

	withTail.Reset()

	d = blockcodecs.NewDecoder(withTail)
	d.SetCheckUnderlyingEOF(true)
	_, err = io.ReadAll(d)
	require.Error(t, err)
}
