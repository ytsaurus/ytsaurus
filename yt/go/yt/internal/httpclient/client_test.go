package httpclient

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorDecoding(t *testing.T) {
	h := http.Header{}

	ytErr, err := decodeYTErrorFromHeaders(h)
	require.NoError(t, err)
	require.Nil(t, ytErr)
}

func TestDecodeFidAndTid(t *testing.T) {
	h := http.Header{
		http.CanonicalHeaderKey("X-YT-Error"): []string{`{"message": "error", "attributes": {"trace_id": 1197470164874488892, "span_id": 298838154694968802}}`},
	}

	ytErr, err := decodeYTErrorFromHeaders(h)
	require.NoError(t, err)
	require.NotNil(t, ytErr)

	require.Equal(t, json.Number("1197470164874488892"), ytErr.Attributes["trace_id"])
	require.Equal(t, json.Number("298838154694968802"), ytErr.Attributes["span_id"])
}
