package httpclient

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

func TestWriteParamsCanReplayPostBody(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://example.test/api/v4/commit_transaction", http.NoBody)
	require.NoError(t, err)

	client := &httpClient{}
	call := &internal.Call{Params: internal.NewCommitTxParams(yt.TxID{}, nil)}
	require.NoError(t, client.writeParams(req, call))
	require.NotNil(t, req.GetBody)

	original, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.NoError(t, req.Body.Close())
	require.NotEmpty(t, original)
	require.Equal(t, int64(len(original)), req.ContentLength)

	replay, err := req.GetBody()
	require.NoError(t, err)
	replayed, err := io.ReadAll(replay)
	require.NoError(t, err)
	require.NoError(t, replay.Close())
	require.Equal(t, original, replayed)
}
