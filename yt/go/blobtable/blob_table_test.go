package blobtable

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yttest"
)

type TestBlobRow struct {
	Key       int    `yson:"key,key"`
	PartIndex int    `yson:"part_index"`
	Data      []byte `yson:"data"`
}

var testBlobRows = []TestBlobRow{
	{Key: 0, PartIndex: 0, Data: []byte("foo")},
	{Key: 0, PartIndex: 1, Data: []byte("bar")},
	{Key: 1, PartIndex: 0, Data: []byte("foo")},
	{Key: 1, PartIndex: 1, Data: []byte("")},
	{Key: 1, PartIndex: 2, Data: []byte("bar")},
	{Key: 2, PartIndex: 0, Data: []byte("foobar")},
	{Key: 3, PartIndex: 0, Data: bytes.Repeat([]byte("foobar"), 1<<20)},
}

func TestReadBlobTable(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	blobTablePath := env.TmpPath()

	require.NoError(t, env.UploadSlice(blobTablePath, testBlobRows))

	br, err := ReadBlobTable(env.Ctx, env.YT, blobTablePath)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		require.True(t, br.Next())

		var key TestBlobRow
		require.NoError(t, br.ScanKey(&key))
		require.Equal(t, i, key.Key)

		data, err := io.ReadAll(br)
		require.NoError(t, err)
		require.Equal(t, []byte("foobar"), data)
	}

	require.True(t, br.Next())

	data, err := io.ReadAll(br)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("foobar"), 1<<20), data)

	require.False(t, br.Next())
	require.NoError(t, br.Err())
}
